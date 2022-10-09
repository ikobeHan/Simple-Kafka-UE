#include "SimpleKafkaConsumer.h"
#include "CoreMinimal.h"
#include <algorithm>


SimpleKafkaConsumer::SimpleKafkaConsumer(const std::string& brokers, const std::string& groupID, const std::string& topics, int partition, int offset)
	: m_strBrokers(brokers),
	m_strTopic(topics),
	m_strGroupid(groupID),
	m_nCurrentOffset(offset),
	m_nPartition(partition)
{
	isMultiTopic = false;
}

SimpleKafkaConsumer::~SimpleKafkaConsumer()
{
	Stop();
}

SimpleKafkaConsumer::SimpleKafkaConsumer(const std::string& brokers, const std::string& groupID, const std::vector<std::string> topics)
	: m_strBrokers(brokers),
	topics(topics),
	m_strGroupid(groupID)
{
	isMultiTopic = true;
}

bool SimpleKafkaConsumer::InitTopics()
{
	RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	RdKafka::Conf* tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
	if (!conf || !tconf) {
		return false;
	}

	std::string errstr;

	/*设置broker list*/
	if (conf->set("metadata.broker.list", m_strBrokers, errstr) != RdKafka::Conf::CONF_OK) {
		std::cerr << "RdKafka conf set brokerlist failed ::" << errstr.c_str() << std::endl;
		return false;
	}

	/*设置consumer group*/
	if (conf->set("group.id", m_strGroupid, errstr) != RdKafka::Conf::CONF_OK) {
		std::cerr << "RdKafka conf set group.id failed :" << errstr.c_str() << std::endl;
		return false;
	}

	ConsumerRebalanceCb ex_rebalance_cb;
	conf->set("rebalance_cb", &ex_rebalance_cb, errstr);

	conf->set("enable.partition.eof", "true", errstr);

	ConsumerEventCb ex_event_cb;
	conf->set("event_cb", &ex_event_cb, errstr);

	if (tconf->set("auto.offset.reset", "latest", errstr) != RdKafka::Conf::CONF_OK) {
		std::cerr << "RdKafka conf set auto.offset.reset failed:" << errstr.c_str() << std::endl;
		return false;
	}

	conf->set("default_topic_conf", tconf, errstr);
	delete tconf;


	/*
	* Create consumer using accumulated global configuration.
	*/
	m_MultiKafkaConsumer = RdKafka::KafkaConsumer::create(conf, errstr);
	if (!m_MultiKafkaConsumer) {
		std::cerr << "Failed to create consumer: " << errstr << std::endl;
		return false;
	}
	delete conf;


	////基本思路为先获取server端的状态信息，将与订阅相关的topic找出来，根据分区，创建TopicPartion；最后使用assign消费
	//RdKafka::Metadata* metadataMap{ nullptr };
	//RdKafka::ErrorCode err = m_MultiKafkaConsumer->metadata(true, nullptr, &metadataMap, 1000);
	//if (err != RdKafka::ERR_NO_ERROR) {
	//	std::cerr << "Failed to create metadataMap"<< std::endl;
	//	return false;
	//}
	//const RdKafka::Metadata::TopicMetadataVector* topicList = metadataMap->topics();

	//RdKafka::Metadata::TopicMetadataVector subTopicMetaVec;
	//copy_if(topicList->begin(), topicList->end(), std::back_inserter(subTopicMetaVec), [this](const RdKafka::TopicMetadata* data) {
	//	return std::find_if(topics.begin(), topics.end(), [data](const std::string& tname) {return data->topic() == tname; }) != topics.end();
	//	});
	//std::vector<RdKafka::TopicPartition*> topicpartions;
	//std::for_each(subTopicMetaVec.begin(), subTopicMetaVec.end(), [&topicpartions](const RdKafka::TopicMetadata* data) {
	//	auto parVec = data->partitions();
	//	std::for_each(parVec->begin(), parVec->end(), [&](const RdKafka::PartitionMetadata* value) {
	//		topicpartions.push_back(RdKafka::TopicPartition::create(data->topic(), value->id(), RdKafka::Topic::OFFSET_END));
	//		});
	//	});
	//m_MultiKafkaConsumer->assign(topicpartions);

	/*
	 * Subscribe to topics
	 */
	RdKafka::ErrorCode err1 = m_MultiKafkaConsumer->subscribe(topics);
	if (err1) {
		std::cerr << "Failed to subscribe to " << topics.size() << " topics: "
			<< RdKafka::err2str(err1) << std::endl;
		return false;
	}

	return true;
}

bool SimpleKafkaConsumer::Init()
{
	if (isMultiTopic)
	{
		return InitTopics();
	}

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

	if (tconf->set("auto.offset.reset", "latest", errstr) != RdKafka::Conf::CONF_OK) {
		std::cerr << "RdKafka conf set auto.offset.reset failed:" << errstr.c_str() << std::endl;
	}

	m_pTopic = RdKafka::Topic::create(m_pKafkaConsumer, m_strTopic, tconf, errstr);
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
}

void SimpleKafkaConsumer::StartAsync(int timeout_ms)
{
	m_bRun = true;
	pmThread = new FPullMessageThread(this, timeout_ms);
}

void SimpleKafkaConsumer::SetPauseAsyc(bool Pause)
{
	if (pmThread)
	{
		pmThread->pauseThread(Pause);
	}
}

void SimpleKafkaConsumer::PullMessage_AnyThread(int timeout_ms)
{
	if (isMultiTopic)
	{
		RdKafka::Message* msg = nullptr;
		while (m_bRun && m_MultiKafkaConsumer) {

			msg = m_MultiKafkaConsumer->consume(timeout_ms);

			ConsumeMessage(msg, nullptr);
			m_pKafkaConsumer->poll(10);

			delete msg;
		}
	}
	else
	{
		RdKafka::Message* msg = nullptr;
		while (m_bRun && m_pKafkaConsumer) {

			msg = m_pKafkaConsumer->consume(m_pTopic, m_nPartition, timeout_ms);

			ConsumeMessage(msg, nullptr);

			m_pKafkaConsumer->poll(10);

			delete msg;
		}
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
		std::string  topicName = message->topic_name();

		//解析Message 带回来的数据,通过代理抛出去
		MsgCallBackHandle.ExecuteIfBound(FString(UTF8_TO_TCHAR(topicName.c_str())), MyFString);

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
	if (pmThread)
	{
		pmThread->StopThread();
		delete pmThread; //析构会自己去调Kill thread
		pmThread = nullptr;
	}

	if (m_pKafkaConsumer)
	{
		m_pKafkaConsumer->stop(m_pTopic, m_nPartition);
		m_pKafkaConsumer->poll(1000);
	}

	if (m_MultiKafkaConsumer)
	{
		m_MultiKafkaConsumer->close();
		delete m_MultiKafkaConsumer;
		m_MultiKafkaConsumer = nullptr;
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

