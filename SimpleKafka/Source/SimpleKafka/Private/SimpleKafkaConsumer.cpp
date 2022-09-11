#include "SimpleKafkaConsumer.h"

#include "CoreMinimal.h"


SimpleKafkaConsumer::SimpleKafkaConsumer(const std::string& brokers, const std::string& groupID, const std::string& topics, int partition, int offset)
	: m_strBrokers(brokers),
	m_strTopics(topics),
	m_strGroupid(groupID),
	m_nCurrentOffset(offset),
	m_nPartition(partition)
{
}

SimpleKafkaConsumer::~SimpleKafkaConsumer()
{
	Stop();
}


bool SimpleKafkaConsumer::Init()
{
	RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	if (!conf) {
		std::cerr << "RdKafka create global conf failed" << std::endl;
		return false;
	}

	std::string errstr;

	/*设置broker list*/
	if (conf->set("metadata.broker.list", m_strBrokers, errstr) != RdKafka::Conf::CONF_OK) {
		std::cerr << "RdKafka conf set brokerlist failed ::" << errstr.c_str() << std::endl;
	}

	/*设置consumer group*/
	if (conf->set("group.id", m_strGroupid, errstr) != RdKafka::Conf::CONF_OK) {
		std::cerr << "RdKafka conf set group.id failed :" << errstr.c_str() << std::endl;
	}

	///*每次从单个分区中拉取消息的最大尺寸*/
	//std::string strfetch_num = "10240000";
	//if (conf->set("max.partition.fetch.bytes", strfetch_num, errstr) != RdKafka::Conf::CONF_OK) {
	//	std::cerr << "RdKafka conf set max.partition failed :" << errstr.c_str() << std::endl;
	//}

	ConsumerEventCb ex_event_cb;
	conf->set("event_cb", &ex_event_cb, errstr);

	/*创建kafka consumer实例*/
	m_pKafkaConsumer = RdKafka::Consumer::create(conf, errstr);
	if (!m_pKafkaConsumer) {
		std::cerr << "failed to ceate consumer" << std::endl;
		return false;
	}
	delete conf;

	RdKafka::Conf* tconf = nullptr;
	/*创建kafka topic的配置*/
	tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
	if (!tconf) {
		std::cerr << "RdKafka create topic conf failed" << std::endl;
		return false;
	}

	if (tconf->set("auto.offset.reset", "smallest", errstr) != RdKafka::Conf::CONF_OK) {
		std::cerr << "RdKafka conf set auto.offset.reset failed:" << errstr.c_str() << std::endl;
	}

	m_pTopic = RdKafka::Topic::create(m_pKafkaConsumer, m_strTopics, tconf, errstr);
	if (!m_pTopic) {
		std::cerr << "RdKafka create topic failed :" << errstr.c_str() << std::endl;
		return false;
	}
	delete tconf;

	RdKafka::ErrorCode resp = m_pKafkaConsumer->start(m_pTopic, m_nPartition, m_nLastOffset);
	if (resp != RdKafka::ERR_NO_ERROR) {
		std::cerr << "failed to start consumer : " << errstr.c_str() << std::endl;
		return false;
	}
	return true;
}

void SimpleKafkaConsumer::Start(int timeout_ms)
{
	RdKafka::Message* msg = nullptr;
	m_bRun = true;
	while (m_bRun) {

		msg = m_pKafkaConsumer->consume(m_pTopic, m_nPartition, timeout_ms);
		ConsumeMessage(msg, nullptr);
		m_pKafkaConsumer->poll(0);
		delete msg;
	}

	//m_pKafkaConsumer->stop(m_pTopic, m_nPartition);
	//if (m_pTopic) {
	//	delete m_pTopic;
	//	m_pTopic = nullptr;
	//}
	//if (m_pKafkaConsumer) {
	//	delete m_pKafkaConsumer;
	//	m_pKafkaConsumer = nullptr;
	//}
	///*销毁kafka实例*/
	//RdKafka::wait_destroyed(5000);
}

void SimpleKafkaConsumer::StartAsync(int timeout_ms)
{
	m_bRun = true;
	pmThread = new FPullMessageThread(this, timeout_ms);
}

void SimpleKafkaConsumer::PullMessage_AnyThread(int timeout_ms)
{
	RdKafka::Message* msg = nullptr;
	while (m_bRun && m_pKafkaConsumer) {

		msg = m_pKafkaConsumer->consume(m_pTopic, m_nPartition, timeout_ms);

		ConsumeMessage(msg, nullptr);

		m_pKafkaConsumer->poll(50);

		delete msg;
	}
}


void SimpleKafkaConsumer::ConsumeMessage(RdKafka::Message* message, void* opt)
{
	switch (message->err()) {

	case RdKafka::ERR__TIMED_OUT:
		break;
	case RdKafka::ERR_NO_ERROR:
	{
		/*std::cout << message->topic_name() << " offset:" << message->offset() << "  partion: " 
			<< message->partition() << " message: " << reinterpret_cast<char*>(message->payload()) << std::endl;*/
		char* tempMsg = reinterpret_cast<char*>(message->payload());
		FString MyFString = FString(UTF8_TO_TCHAR(tempMsg));
		//UE_LOG(LogTemp, Log, TEXT("message back--->  %s"),*MyFString);
		//GEngine->AddOnScreenDebugMessage(-1, 10, FColor::Blue, MyFString);

		m_nLastOffset = message->offset();

		//解析Message 带回来的数据,通过代理抛出去
		MsgCallBackHandle.ExecuteIfBound(MyFString);

		break;
	}
	case RdKafka::ERR__PARTITION_EOF:
		UE_LOG(LogTemp, Warning, TEXT("Reached the end of the queue, offset:%d"), m_nLastOffset);
		break;
	case RdKafka::ERR__UNKNOWN_TOPIC:
	case RdKafka::ERR__UNKNOWN_PARTITION:
		UE_LOG(LogTemp, Warning, TEXT("Consume failed:: %s"), message->errstr().c_str());
		Stop();
		break;
	default:
		UE_LOG(LogTemp, Warning, TEXT("Consume default failed::%s"), message->errstr().c_str());
		Stop();
		break;
	}
}


void SimpleKafkaConsumer::Stop()
{
	UE_LOG(LogTemp, Warning, TEXT("message end---> SimpleKafkaConsumer::Stop"));

	m_bRun = false; 
	pmThread = nullptr;

	if (m_pKafkaConsumer)
	{
		m_pKafkaConsumer->stop(m_pTopic, m_nPartition);
		m_pKafkaConsumer->poll(1000);
	}

	if (m_pTopic) {
		delete m_pTopic;
		m_pTopic = nullptr;
	}

	if (m_pKafkaConsumer) {
		delete m_pKafkaConsumer;
		m_pKafkaConsumer = nullptr;
	}

	/*销毁kafka实例*/
	RdKafka::wait_destroyed(5000);
}

