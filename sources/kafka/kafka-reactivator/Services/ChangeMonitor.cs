using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Confluent.Kafka;
using Drasi.Source.SDK;
using Drasi.Source.SDK.Models;

namespace Reactivator.Services;

class ChangeMonitor : IChangeMonitor
{
    private readonly IStateStore _stateStore;
    private readonly IConfiguration _configuration;
    private readonly IEventMapper _eventMapper;
    private readonly string _consumerGroup;
    private readonly ILogger<ChangeMonitor> _logger;
    private readonly string[] _topicList;

    public ChangeMonitor(IStateStore stateStore, IConfiguration configuration, IEventMapper eventMapper, ILogger<ChangeMonitor> logger)
    {
        _stateStore = stateStore;
        _configuration = configuration;
        _eventMapper = eventMapper;
        _logger = logger;

        var topics = configuration["topics"] ?? "";
        _consumerGroup = configuration["consumerGroup"] ?? "drasi-reactivator";
        _topicList = topics.Split(',', StringSplitOptions.RemoveEmptyEntries);        
    }

    public async IAsyncEnumerable<SourceChange> Monitor([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = Channel.CreateBounded<SourceChange>(1);
        var consumers = new List<KafkaConsumer>();
        var tasks = new List<Task>();

        foreach (var topic in _topicList)
        {
            var consumer = new KafkaConsumer(channel, _stateStore, _eventMapper, _configuration, _logger, topic); 
            consumers.Add(consumer);
            tasks.Add(consumer.StartAsync(cancellationToken));
        }

        await foreach (var change in channel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return change;
        }
        
        await Task.WhenAll(tasks);
    }
}