#include "SimpleKafkaProducer.h"


//("192.168.0.105:9092", "topic_demo", 0)
SimpleKafkaProducer::SimpleKafkaProducer(const std::string& brokers, const std::string& topic, int partition) {
    m_brokers = brokers;
    m_topicStr = topic;
    m_partition = partition;
}


/*
另外几个关键的参数：
partition.assignment.strategy   : range,roundrobin  消费者客户端partition分配策略，当被选举为leader时，分配partition分区给组员消费者的策略
*/
bool SimpleKafkaProducer::Init()
{
	//先填充构造生产者客户端的参数配置：
	m_config = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	if (m_config == nullptr) {
		std::cout << "Create Rdkafka Global Conf Failed." << std::endl;
		return false;
	}

	m_topicConfig = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
	if (m_topicConfig == nullptr) {
		std::cout << "Create Rdkafka Topic Conf Failed." << std::endl;
		return false;
	}

	//下面开始配置各种需要的配置项：
	RdKafka::Conf::ConfResult   result;
	std::string                 error_str;

	result = m_config->set("booststrap.servers", m_brokers, error_str); //设置生产者待发送服务器的地址: "ip:port" 格式
	if (result != RdKafka::Conf::CONF_OK) {
		std::cout << "Global Conf set 'booststrap.servers' failed: " << error_str << std::endl;
	}

	result = m_config->set("statistics.interval.ms", "10000", error_str);
	if (result != RdKafka::Conf::CONF_OK) {
		std::cout << "Global Conf set ‘statistics.interval.ms’ failed: " << error_str << std::endl;
	}

	result = m_config->set("message.max.bytes", "10240000", error_str);     //设置发送端发送的最大字节数，如果发送的消息过大则返回失败
	if (result != RdKafka::Conf::CONF_OK) {
		std::cout << "Global Conf set 'message.max.bytes' failed: " << error_str << std::endl;
	}


	m_dr_cb = new ProducerDeliveryReportCb;
	result = m_config->set("dr_cb", m_dr_cb, error_str);    //设置每个消息发送后的发送结果回调
	if (result != RdKafka::Conf::CONF_OK) {
		std::cout << "Global Conf set ‘dr_cb’ failed: " << error_str << std::endl;
	}

	m_event_cb = new ProducerEventCb;
	result = m_config->set("event_cb", m_event_cb, error_str);
	if (result != RdKafka::Conf::CONF_OK) {
		std::cout << "Global Conf set ‘event_cb’ failed: " << error_str << std::endl;
	}

	m_partitioner_cb = new HashPartitionerCb;
	result = m_topicConfig->set("partitioner_cb", m_partitioner_cb, error_str);     //设置自定义分区器
	if (result != RdKafka::Conf::CONF_OK) {
		std::cout << "Topic Conf set ‘partitioner_cb’ failed: " << error_str << std::endl;
	}

	//创建Producer生产者客户端：
	m_producer = RdKafka::Producer::create(m_config, error_str);    //RdKafka::Producer::create(const RdKafka::Conf *conf, std::string &errstr);
	if (m_producer == nullptr) {
		std::cout << "Create Producer failed: " << error_str << std::endl;
		return false;
	}

	//创建Topic对象，后续produce发送消息时需要使用
	m_topic = RdKafka::Topic::create(m_producer, m_topicStr, m_topicConfig, error_str); //RdKafka::Topic::create(Hanle *base, const std::string &topic_str, const Conf *conf, std::string &errstr);
	if (m_topic == nullptr) {
		std::cout << "Create Topic failed: " << error_str << std::endl;
		return false;
	}
	return true;
}


/*
RdKafka::ErrorCode RdKafka::Produce::produce(   RdKafka::Topic *topic, int32_t partition,   //生产消息发往的主题、分区
												int msgflags,                               //RK_MSG_COPY（拷贝payload）、 RK_MSG_FREE（发送后释放payload内容）、 RK_MSG_BLOCK（在消息队列满时阻塞produce函数）
												void *payload, size_t len,                  //消息净荷、长度
												const string &key, void *msg_opaque);       //key; opaque是可选的应用程序提供给每条消息的opaque指针，opaque指针会在dr_cb回调函数内提供，在Kafka内部维护
返回值：
	ERR_NO_ERROR            :   消息成功入队
	ERR_QUEUE_FULL          :   队列满
	ERR_MSG_SIZE_TOO_LARGE  :   消息长度过大
	ERR_UNKNOWN_PARTITION   :   所指定的分区不存在
	ERR_UNKNOWN_TOPIC       :   所指定的主题不存在


int RdKafka::Producer::poll (int timeout_ms);
阻塞等待生产消息发送完成，
poll另外的重要作用是：
（1）轮询处理指定的Kafka句柄的Event（m_producer上的Event事件，在本例中处理事件的方式是进行打印）；
（2）触发应用程序提供的回调函数调用，例如 ProducerDeliveryReportCb 等回调函数都需要poll()进行触发。
*/
bool SimpleKafkaProducer::SendMessage(const std::string& msg, const std::string& key) {
    size_t len = msg.length();
    void* payload = const_cast<void*>(static_cast<const void*>(msg.data()));

    RdKafka::ErrorCode error_code = m_producer->produce(m_topic, 
        RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_FREE,//发送后释放
        payload, 
        len, 
        &key, 
        NULL);

    m_producer->poll(0);        //poll()参数为0意味着不阻塞；poll(0)主要是为了触发应用程序提供的回调函数

    if (error_code != RdKafka::ErrorCode::ERR_NO_ERROR) {
        std::cerr << "Produce failed: " << RdKafka::err2str(error_code) << std::endl;
        if (error_code == RdKafka::ErrorCode::ERR__QUEUE_FULL) {
			UE_LOG(LogTemp,Error,TEXT("## kafka ERR__QUEUE_FULL error"));
            m_producer->poll(1000);     //如果发送失败的原因是队列正满，则阻塞等待一段时间
        }
        else if (error_code == RdKafka::ErrorCode::ERR_MSG_SIZE_TOO_LARGE) {
            //如果发送消息过大，超过了max.size，则需要裁减后重新发送
			UE_LOG(LogTemp, Error, TEXT("## kafka ERR_MSG_SIZE_TOO_LARGE error"));
        }
        else {
            std::cerr << "ERR_UNKNOWN_PARTITION or ERR_UNKNOWN_TOPIC" << std::endl;
        }
    }

	return error_code == RdKafka::ErrorCode::ERR_NO_ERROR;
}



/*
-- flush():
ErrorCode Kafka::Producer::flush(int timeout_ms);
flush会优先调用poll()，去触发生产者提前注册的各种回调函数，然后等待生产者上的所有消息全部发送完毕。

-- outq_len: “出队列”长度，是 Handle 类的一个成员，
表示 生产者队列中中待发送到broker上的数据，或 消费者队列中待发送到broker上的ACK。

Handle是Producer和Consumer类的基类，表示“客户端的句柄”：
	class Producer : public virtual Handle { }
	class Consumer : public virtual Handle { }
	class KafkaConsumer : public virtual Handle { }
*/
void SimpleKafkaProducer::Stop()
{
	while (m_producer->outq_len() > 0) 
	{   //当 Handle->outq_len() 客户端的“出队列” 的长度大于0
		std::cerr << "Waiting for: " << m_producer->outq_len() << std::endl;
		m_producer->flush(5000);
	}

	delete m_config;
	delete m_topicConfig;
	delete m_topic;
	delete m_producer;
	delete m_dr_cb;
	delete m_event_cb;
	delete m_partitioner_cb;
}


SimpleKafkaProducer::~SimpleKafkaProducer() 
{
	Stop();
}


