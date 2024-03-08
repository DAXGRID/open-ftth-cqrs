using FluentResults;
using Microsoft.Extensions.Logging;
using OpenFTTH.EventSourcing;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace OpenFTTH.CQRS
{
    public class CommandDispatcher : ICommandDispatcher
    {
        private readonly static SemaphoreSlim _semaphoreSlim = new SemaphoreSlim(1);

        private readonly ILogger<CommandDispatcher> _logger;
        private readonly IServiceProvider _serviceProvider;
        private readonly IEventStore _eventStore;

        private readonly bool _diableCommandLogging;

        public CommandDispatcher(ILogger<CommandDispatcher> logger, IServiceProvider serviceProvider, IEventStore eventStore, bool diableCommandLogging = false)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
            _eventStore = eventStore;
            _diableCommandLogging = diableCommandLogging;
        }

        public async Task<TResult> HandleAsync<TCommand, TResult>(TCommand command) where TCommand : ICommand<TResult>
        {
            if (_serviceProvider.GetService(typeof(ICommandHandler<TCommand, TResult>)) is not ICommandHandler<TCommand, TResult> service)
                throw new ApplicationException($"The Command Dispatcher cannot find command handler: {typeof(TCommand).Name} Notice that you can use the AddCQRS extension in OpenFTTH.CQRS to easily add command and query handlers.");

            try
            {
                TResult cmdResult = default(TResult);

                try
                {
                    await _semaphoreSlim.WaitAsync();
                    cmdResult = await service.HandleAsync(command);
                }
                catch (Exception)
                {
                    // Rethrow the exception, we want this try catch finally to make sure the sempahoreslim is released in case of failure.
                    throw;
                }
                finally
                {
                    _semaphoreSlim.Release();
                }

                if (command is BaseCommand baseCommand && cmdResult is Result result)
                {
                    if (baseCommand.CorrelationId == Guid.Empty)
                        _logger.LogError($"{typeof(TCommand).Name} command has empty correlation id. Please make sure all initated commands has a correlation id set.");


                    string atNode = "";
                    if (baseCommand.UserContext != null && baseCommand.UserContext.EditingRouteNodeId != Guid.Empty)
                        atNode = " from route node: " + baseCommand.UserContext.EditingRouteNodeId.ToString() + ",";

                    if (result.IsFailed && result.Errors != null)
                    {
                        foreach (var error in result.Errors)
                        {
                            _logger.LogWarning($"{typeof(TCommand).Name} command with id {baseCommand.CmdId}, correlation id: {baseCommand.CorrelationId}, invoked by user: '{baseCommand.UserContext?.UserName}',{atNode} failed with message: {error.Message}");
                        }
                    }
                    else
                    {

                        _logger.LogInformation($"{typeof(TCommand).Name} command with id {baseCommand.CmdId}, correlation id: {baseCommand.CorrelationId}, invoked by user: '{baseCommand.UserContext?.UserName}',{atNode} was successfully processed.");
                    }

                    // Store command in event store
                    if (_eventStore != null && !_diableCommandLogging)
                    {
                        var cmdLogEntry = new CommandLogEntry(baseCommand.CmdId, command, result);
                        _eventStore.CommandLog.Store(cmdLogEntry);
                    }
                }

                return cmdResult;
            }
            catch (Exception ex)
            {
                _logger.LogError("UNHANDLED_COMMAND_EXCEPTION: " + ex.Message, ex);
                throw;
            }
        }
    }
}
