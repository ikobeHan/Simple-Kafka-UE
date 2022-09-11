
#pragma once

#include <string>
#include <iostream>
#include "librdkafka/rdkafkacpp.h"

/*
用法：
SimpleKafkaProducer* KafkaprClient_ = new SimpleKafkaProducer("localhost:9092", "test", 0);
KafkaprClient_->Init();
KafkaprClient_->pushMessage("ddddddd","Anykey"); 

1. KafkaProducer构造函数：初始化一个“生产者客户端”，要指定三个参数：（1）生产者所要连往的Kafka地址，是zookeeper的ip和port；（2）生产者后续生产的消息所要发送的Topic主题；（3）消息所要发往的分区

2. Init 中会设置配置，RdKafka::Conf 是配置接口类，用来设置生产者、消费者、broker的各项配置值：

*/
class SIMPLEKAFKA_API SimpleKafkaProducer {
public:
    explicit SimpleKafkaProducer(const std::string& brokers, const std::string& topic, int partition);       //epplicit：禁止隐式转换，例如不能通过string的构造函数转换出一个broker
    ~SimpleKafkaProducer();

    bool Init();
    bool SendMessage(const std::string& msg, const std::string& key);
    void Stop();

protected:
    std::string     m_brokers;  //IP+端口 127.0.0：8080
    std::string     m_topicStr;  //topic 
    int             m_partition;  //分区

    RdKafka::Conf* m_config;           //RdKafka::Conf --- 配置接口类，用来设置对 生产者、消费者、broker的各项配置值
    RdKafka::Conf* m_topicConfig;

    RdKafka::Producer* m_producer;
    RdKafka::Topic* m_topic;

    RdKafka::DeliveryReportCb* m_dr_cb;            //RdKafka::DeliveryReportCb 用于在调用 RdKafka::Producer::produce() 后返回发送结果，RdKafka::DeliveryReportCb是一个类，需要自行填充其中的回调函数及处理返回结果的方式
    RdKafka::EventCb* m_event_cb;         //RdKafka::EventCb 用于从librdkafka向应用程序传递errors,statistics,logs 等信息的通用接口
    RdKafka::PartitionerCb* m_partitioner_cb;   //Rdkafka::PartitionerCb 用于设定自定义分区器

};




/*
Kafka将会在produce()之后返回发送结果时调用 DeliveryReportCb::dr_cb(), 并将结果填充到message中
*/
class SIMPLEKAFKA_API ProducerDeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
    void dr_cb(RdKafka::Message& message) {  //重载基类RdKafka::DeliveryReportCb中的虚函数dr_cb()
        if (message.err() != 0) {       //发送出错
            std::cerr << "Message delivery failed: " << message.errstr() << std::endl;
        }
        else {                             //发送成功
            std::cerr << "Message delivered to topic: " << message.topic_name()
                << " [" << message.partition()
                << "] at offset " << message.offset() << std::endl;
        }
    }
};


/*
EventCb 是用于从librdkafka向应用程序返回errors,statistics, logs等信息的通用接口：
*/
class SIMPLEKAFKA_API ProducerEventCb : public RdKafka::EventCb {
public:
    void event_cb(RdKafka::Event& event) {
        switch (event.type()) {
        case RdKafka::Event::EVENT_ERROR:
            std::cout << "RdKafka::EVENT::EVENT_ERROR: " << RdKafka::err2str(event.err()) << std::endl;
            break;
        case RdKafka::Event::EVENT_STATS:
            std::cout << "RdKafka::EVENT::EVENT_STATS: " << event.str() << std::endl;
            break;
        case RdKafka::Event::EVENT_LOG:
            std::cout << "RdKafka::EVENT::EVENT_LOG: " << event.fac() << std::endl;
            break;
        case RdKafka::Event::EVENT_THROTTLE:
            std::cout << "RdKafka::EVENT::EVENT_THROTTLE: " << event.broker_name() << std::endl;
            break;
        }

    }
};




/*
用户实现的派生类重载partitioner_cb() 这个函数后，也是要提供给Kafka去调用的，其中参数 partition_cnt 并非由Producer指定，而是Kafka根据Topic创建时的信息去查询，
且Kafka上的Topic创建也不是由Producer生产者客户端创建的，目前已知的方法只有使用 kafka-topics.sh 脚本这一种方法。

关于“创建主题”的描述：
-- 如果broker端配置参数 auto.create.topics.enable 设置为true（默认值为true），
那 么当生产者向一个尚未创建的主题发送消息时，会自动创建一个分区数为 num.partitions（默认值为1）、副本因为为 default.replication.factor（默认值为1）的主题。
-- 除此之外，当一个消费者开始从未知主题中读取消息时，或者当任意一个客户端向未知主题发送元数据请求时，都会按照配置参数 num.partitions 和 default.replication.factor的值创建一个相应主题。
*/
class SIMPLEKAFKA_API HashPartitionerCb : public RdKafka::PartitionerCb {       //自定义生产者分区器，作用就是返回一个分区id。  对key计算Hash值，得到待发送的分区号（其实这跟默认的分区器计算方式是一样的）
public:
    int32_t partitioner_cb(const RdKafka::Topic* topic, const std::string* key,
        int32_t partition_cnt, void* msg_opaque)
    {
        char msg[128] = { 0 };
        sprintf_s(msg, "HashPartitionCb:[%s][%s][%d]", topic->name().c_str(), key->c_str(), partition_cnt);
        std::cout << msg << std::endl;

        //前面的操作只是为了在分区器回调中打印出一行打印，分区器真正的操作是在下面generate_hash，生成一个待发送的分区ID
        return generate_hash(key->c_str(), key->size()) % partition_cnt;
    }
private:
    static inline unsigned int generate_hash(const char* str, size_t len) {
        unsigned int hash = 5381;
        for (size_t i = 0; i < len; i++) {
            hash = ((hash << 5) + hash) + str[i];
        }
        return hash;    //返回值必须在 0 到 partition_cnt 之间。如果出错则发回 PARTITION_UA（-1）
    }
};





