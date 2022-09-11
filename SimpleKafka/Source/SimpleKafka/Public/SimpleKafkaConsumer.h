#pragma once

#include <string>
#include <iostream>
#include <vector>
#include <stdio.h>
#include "librdkafka/rdkafkacpp.h"


/*
* 用法：
SimpleKafkaConsumer* KafkaConsumerClient_ = new SimpleKafkaConsumer();
if (KafkaConsumerClient_->Init()){
    KafkaConsumerClient_->RegisterCallback(Delegate); //optional
	KafkaConsumerClient_->Start(1000);
}

。。。

KafkaConsumerClient_->stop();

*/
static volatile bool run = true;

DECLARE_DELEGATE_OneParam(FKafkaConsumerCallbackHandle, FString);

class SIMPLEKAFKA_API SimpleKafkaConsumer {
public:
    explicit SimpleKafkaConsumer(const std::string& brokers, const std::string& groupID, const std::string& topics, int partition = 0, int offset = 0);
    ~SimpleKafkaConsumer();

    
    bool Init();

    UE_DEPRECATED(4.27,"以同步方式启动消息消费，UE客户端不推荐，会阻塞主线程")
    void Start(int timeout_ms);

    void StartAsync(int timeout_ms);

    void Stop();

	void RegisterCallback(FKafkaConsumerCallbackHandle Delegate)
	{
		MsgCallBackHandle = Delegate;
	};

	FKafkaConsumerCallbackHandle GetCallbackHandle()
	{
		return MsgCallBackHandle;
	};

	bool isRun() {return m_bRun;};

    void PullMessage_AnyThread(int timeout_ms);

private:
    void ConsumeMessage(RdKafka::Message* message, void* opt);

    FKafkaConsumerCallbackHandle MsgCallBackHandle;

protected:
	std::string m_strBrokers;

	std::string m_strTopics;

	std::string m_strGroupid;

	int64_t m_nLastOffset = 0;

	RdKafka::Consumer* m_pKafkaConsumer = nullptr;

	RdKafka::Topic* m_pTopic = nullptr;

	int64_t           m_nCurrentOffset = RdKafka::Topic::OFFSET_BEGINNING;

	int32_t           m_nPartition = 0;

	bool m_bRun = false;  //是否持续消费，持续监听

    class FPullMessageThread* pmThread = nullptr;
};


class SIMPLEKAFKA_API ConsumerEventCb : public RdKafka::EventCb {
public:
    void event_cb(RdKafka::Event& event) {
        switch (event.type())
        {
        case RdKafka::Event::EVENT_ERROR:
            std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " << event.str() << std::endl;
            break;
        case RdKafka::Event::EVENT_STATS:
            std::cerr << "\"STATS\": " << event.str() << std::endl;
            break;
        case RdKafka::Event::EVENT_LOG:
            fprintf(stderr, "LOG-%i-%s: %s\n", event.severity(), event.fac().c_str(), event.str().c_str());
            break;
        case RdKafka::Event::EVENT_THROTTLE:
            std::cerr << "THROTTLED: " << event.throttle_time() << "ms by " << event.broker_name() << " id " << (int)event.broker_id() << std::endl;
            break;
        default:
            std::cerr << "EVENT " << event.type() << " (" << RdKafka::err2str(event.err()) << "): " << event.str() << std::endl;
            break;
        }
    }
};


/*
同时包括 主题 和 分区信息，所以 consumer.assign(); 订阅分区的方式是包括不同主题的不同分区的集合。
*/
class SIMPLEKAFKA_API ConsumerRebalanceCb : public RdKafka::RebalanceCb {
public:
    void rebalance_cb(RdKafka::KafkaConsumer* consumer, RdKafka::ErrorCode err,
        std::vector<RdKafka::TopicPartition*>& partitions)		//Kafka服务端通过 err参数传入再均衡的具体事件（发生前、发生后），通过partitions参数传入再均衡 前/后，旧的/新的 分区信息
    {
        //std::cerr << "RebalanceCb: " << RdKafka::err2str(err) << ": " << printTopicPartition(partitions);

        if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {		//ERR__ASSIGN_PARTITIONS: 表示“再均衡发生之后，消费者开始消费之前”，此时消费者客户端可以从broker上重新加载offset
            consumer->assign(partitions);					//再均衡后，重新 assign() 订阅这些分区
            partition_count = (int)partitions.size();
        }
        else if (err == RdKafka::ERR__REVOKE_PARTITIONS) {		//ERR__REVOKE_PARTITIONS: 表示“消费者停止消费之后，再均衡发生之前”，此时应用程序可以在这里提交 offset
            consumer->unassign();								//再均衡前，unassign() 退订这些分区
            partition_count = 0;								//退订所有分区后，清0
        }
        else {
            std::cerr << "Rebalancing error: " << RdKafka::err2str(err) << std::endl;
        }
    }

private:
    static void printTopicPartition(const std::vector<RdKafka::TopicPartition*>& partitions) {	//打印出所有的主题、分区信息
        for (unsigned int i = 0; i < partitions.size(); i++) {
            std::cerr << partitions[i]->topic() << "[" << partitions[i]->partition() << "], ";
        }
        std::cerr << "\n";
    }
private:
    int partition_count;			//保存consumer消费者客户端 当前订阅的分区数
};




/* asynchronous Thread for pull msg from server */
class SIMPLEKAFKA_API FPullMessageThread : public FRunnable {

public:
	FPullMessageThread(SimpleKafkaConsumer* InKafkaConsumer,int32 Timeout) :
		KafkaConsumer(InKafkaConsumer),
        Timeout_ms(Timeout)
	{
		FString threadName = "FPullMessageThread" + FGuid::NewGuid().ToString();
		thread = FRunnableThread::Create(this, *threadName, 0, EThreadPriority::TPri_Normal);
	}
	virtual uint32 Run() override 
    {

		if (KafkaConsumer == nullptr) {
			UE_LOG(LogTemp, Error, TEXT("KafkaConsumer Class is not initialized."));
			return 0;
		}

		while (KafkaConsumer->isRun())
		{
            KafkaConsumer->PullMessage_AnyThread(Timeout_ms);
		}

		thread = nullptr;
		return 0;
	}


	void pauseThread(bool pause) {
		paused = pause;
		if (thread != nullptr)
			thread->Suspend(pause);
	}


protected:

	FRunnableThread* thread = nullptr;

	SimpleKafkaConsumer* KafkaConsumer = nullptr;
	RdKafka::Message* msg = nullptr;

    int32 Timeout_ms = 0;

	bool paused;
};


