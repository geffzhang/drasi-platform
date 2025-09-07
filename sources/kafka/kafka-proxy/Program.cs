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

using Drasi.Source.SDK;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Proxy.Services;

// Create the proxy builder
var builder = new SourceProxyBuilder()
    .UseBootstrapHandler<BootstrapHandler>()
    .ConfigureServices(services => 
    {
        // Register event mapper factory and mappers
        services.AddSingleton<EventMapperFactory>();
        services.AddTransient<JsonEventMapper>();
        services.AddTransient<AvroEventMapper>();
        services.AddTransient<PlainTextEventMapper>();
        
        // Register the appropriate mapper based on configuration
        services.AddTransient<IEventMapper>(sp => 
        {
            var factory = sp.GetRequiredService<EventMapperFactory>();
            var configuration = sp.GetRequiredService<IConfiguration>();
            var format = configuration.GetValue<string>("messageFormat", "json");
            return factory.GetMapper(format);
        });
    });

// Build and start the proxy
var proxy = builder.Build();

// Log available message formats
var logger = proxy.Services.GetRequiredService<ILogger<EventMapperFactory>>();
var mapperFactory = proxy.Services.GetRequiredService<EventMapperFactory>();
var formats = string.Join(", ", mapperFactory.GetAvailableFormats());
logger.LogInformation("Kafka proxy starting with available message formats: {Formats}", formats);

// Start the proxy
await proxy.StartAsync();