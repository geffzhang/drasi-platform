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
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Reactivator.Configuration;
using Reactivator.Services;
using System;

// Create the reactivator builder
var builder = new ReactivatorBuilder()
    .UseChangeMonitor<ChangeMonitor>()
    .UseDeprovisionHandler<DeprovisionHandler>()
    .ConfigureServices(services => 
    {
        // Register configuration
        services.AddSingleton<KafkaConfiguration>();
        
        // Register metrics and telemetry
        services.AddSingleton<KafkaMetrics>();
        
        // Register retry policy
        services.AddSingleton<RetryPolicy>();
        
        // Register dead letter handler
        services.AddSingleton<DeadLetterHandler>();
        
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
        
        // Add health checks
        services.AddHealthChecks()
            .AddCheck<KafkaHealthCheck>("kafka", 
                failureStatus: HealthStatus.Degraded,
                tags: new[] { "kafka", "messaging" });
        
        // Configure OpenTelemetry
        services.AddOpenTelemetry()
            .ConfigureResource(resource => resource
                .AddService("kafka-reactivator"))
            .WithTracing(tracing => tracing
                .AddAspNetCoreInstrumentation()
                .AddSource("Drasi.Kafka.Reactivator"))
            .WithMetrics(metrics => metrics
                .AddAspNetCoreInstrumentation()
                .AddMeter("Drasi.Kafka.Reactivator"));
    });

// Build and start the reactivator
var reactivator = builder.Build();

// Initialize metrics
var logger = reactivator.Services.GetRequiredService<ILogger<KafkaMetrics>>();
var kafkaMetrics = reactivator.Services.GetRequiredService<KafkaMetrics>();
kafkaMetrics.Initialize(logger);

// Log available message formats
var mapperFactory = reactivator.Services.GetRequiredService<EventMapperFactory>();
var formats = string.Join(", ", mapperFactory.GetAvailableFormats());
logger.LogInformation("Kafka reactivator starting with available message formats: {Formats}", formats);

// Start the reactivator
await reactivator.StartAsync();