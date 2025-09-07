// Copyright 2024 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Reactivator.Services
{
    /// <summary>
    /// Implements an exponential backoff retry policy with circuit breaker pattern
    /// </summary>
    public class RetryPolicy
    {
        private readonly ILogger _logger;
        private readonly int _maxRetries;
        private readonly int _initialDelayMs;
        private readonly int _maxDelayMs;
        private readonly double _backoffFactor;
        private readonly int _circuitBreakerThreshold;
        
        private int _consecutiveFailures;
        private bool _circuitOpen;
        private DateTime _circuitResetTime;

        public RetryPolicy(
            ILogger logger, 
            int maxRetries = 10, 
            int initialDelayMs = 1000, 
            int maxDelayMs = 60000, 
            double backoffFactor = 2.0,
            int circuitBreakerThreshold = 5)
        {
            _logger = logger;
            _maxRetries = maxRetries;
            _initialDelayMs = initialDelayMs;
            _maxDelayMs = maxDelayMs;
            _backoffFactor = backoffFactor;
            _circuitBreakerThreshold = circuitBreakerThreshold;
            
            _consecutiveFailures = 0;
            _circuitOpen = false;
        }

        /// <summary>
        /// Executes the provided action with retry logic
        /// </summary>
        public async Task ExecuteWithRetryAsync(Func<Task> action, string operationName, CancellationToken cancellationToken)
        {
            int retryCount = 0;
            int delay = _initialDelayMs;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                
                // Check if circuit breaker is open
                if (_circuitOpen)
                {
                    if (DateTime.UtcNow < _circuitResetTime)
                    {
                        _logger.LogWarning("Circuit breaker open for {OperationName}, waiting until {ResetTime}", 
                            operationName, _circuitResetTime);
                        await Task.Delay(Math.Min(5000, (int)(_circuitResetTime - DateTime.UtcNow).TotalMilliseconds), cancellationToken);
                        continue;
                    }
                    
                    // Try to reset circuit breaker
                    _logger.LogInformation("Attempting to reset circuit breaker for {OperationName}", operationName);
                    _circuitOpen = false;
                }

                try
                {
                    await action();
                    
                    // Success - reset failure counter
                    _consecutiveFailures = 0;
                    return;
                }
                catch (Exception ex) when (IsTransientException(ex))
                {
                    // Handle transient exceptions with retry
                    _consecutiveFailures++;
                    
                    if (retryCount >= _maxRetries)
                    {
                        _logger.LogError(ex, "Maximum retry attempts ({MaxRetries}) reached for {OperationName}", 
                            _maxRetries, operationName);
                        throw;
                    }

                    // Check if we need to open the circuit breaker
                    if (_consecutiveFailures >= _circuitBreakerThreshold)
                    {
                        _circuitOpen = true;
                        _circuitResetTime = DateTime.UtcNow.AddSeconds(30); // Open circuit for 30 seconds
                        _logger.LogWarning("Circuit breaker opened for {OperationName} after {ConsecutiveFailures} consecutive failures", 
                            operationName, _consecutiveFailures);
                        continue;
                    }

                    _logger.LogWarning(ex, "Transient error in {OperationName} (attempt {RetryCount} of {MaxRetries}), retrying in {Delay}ms: {Message}", 
                        operationName, retryCount + 1, _maxRetries, delay, ex.Message);
                    
                    await Task.Delay(delay, cancellationToken);
                    
                    // Increase delay with exponential backoff, but cap at max delay
                    delay = Math.Min(_maxDelayMs, (int)(delay * _backoffFactor));
                    retryCount++;
                }
                catch (Exception ex)
                {
                    // Non-transient exception - don't retry
                    _logger.LogError(ex, "Non-transient error in {OperationName}: {Message}", operationName, ex.Message);
                    throw;
                }
            }
        }

        /// <summary>
        /// Determines if an exception is transient and should be retried
        /// </summary>
        private bool IsTransientException(Exception ex)
        {
            // Kafka-specific transient errors
            if (ex is Confluent.Kafka.KafkaException kafkaEx)
            {
                // Retry on broker connection issues, timeout errors, etc.
                return kafkaEx.Error.IsLocalError || 
                       kafkaEx.Error.IsBrokerError || 
                       kafkaEx.Error.IsError;
            }
            
            // Network-related errors
            if (ex is System.Net.Sockets.SocketException || 
                ex is System.IO.IOException || 
                ex is TimeoutException)
            {
                return true;
            }
            
            // Task cancellation is not transient
            if (ex is TaskCanceledException || ex is OperationCanceledException)
            {
                return false;
            }
            
            // By default, consider unknown exceptions as non-transient
            return false;
        }
    }
}