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
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Reactivator.Services
{
    /// <summary>
    /// Factory for creating event mappers based on message format
    /// </summary>
    public class EventMapperFactory
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<EventMapperFactory> _logger;
        private readonly Dictionary<string, Type> _mapperTypes = new(StringComparer.OrdinalIgnoreCase);
        private readonly string _defaultFormat;

        public EventMapperFactory(
            IServiceProvider serviceProvider, 
            ILogger<EventMapperFactory> logger,
            IConfiguration configuration)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            _defaultFormat = configuration.GetValue<string>("messageFormat", "json");
            
            // Register all available mappers
            RegisterMapper<JsonEventMapper>();
            RegisterMapper<PlainTextEventMapper>();
            RegisterMapper<BinaryEventMapper>();
        }

        /// <summary>
        /// Register a mapper type
        /// </summary>
        private void RegisterMapper<T>() where T : IEventMapper
        {
            try
            {
                // Create an instance to get the format
                var mapper = ActivatorUtilities.CreateInstance<T>(_serviceProvider);
                _mapperTypes[mapper.Format] = typeof(T);
                _logger.LogInformation("Registered event mapper for format: {Format}", mapper.Format);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to register event mapper {MapperType}", typeof(T).Name);
            }
        }

        /// <summary>
        /// Get a mapper for the specified format
        /// </summary>
        public IEventMapper GetMapper(string format = null)
        {
            format ??= _defaultFormat;
            
            if (_mapperTypes.TryGetValue(format, out var mapperType))
            {
                try
                {
                    return (IEventMapper)ActivatorUtilities.CreateInstance(_serviceProvider, mapperType);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to create event mapper for format {Format}", format);
                }
            }
            
            // Fall back to JSON if the requested format is not available
            _logger.LogWarning("Event mapper for format {Format} not found, falling back to JSON", format);
            return ActivatorUtilities.CreateInstance<JsonEventMapper>(_serviceProvider);
        }

        /// <summary>
        /// Get all available formats
        /// </summary>
        public IEnumerable<string> GetAvailableFormats()
        {
            return _mapperTypes.Keys;
        }
    }
}