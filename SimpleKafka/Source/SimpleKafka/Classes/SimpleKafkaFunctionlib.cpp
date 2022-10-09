#include "SimpleKafkaFunctionlib.h"
#include "SimpleKafkaConsumer.h"
#include "SimpleKafkaProducer.h"


SimpleKafkaConsumer* USimpleKafkaFunctionLib::KafkaConsumerClient_ = nullptr;
SimpleKafkaProducer* USimpleKafkaFunctionLib::KafkaProducer = nullptr;

bool USimpleKafkaFunctionLib::StartConsumer(const FString& URL, const FString& TopicStr, const FString& GroudID)
{
	if (KafkaConsumerClient_) //单例吧。只需要监听一个topic
	{
		return false;
	}

	KafkaConsumerClient_ = new SimpleKafkaConsumer(TCHAR_TO_UTF8(*URL), TCHAR_TO_UTF8(*GroudID), TCHAR_TO_UTF8(*TopicStr));
	if (KafkaConsumerClient_->Init()) 
	{
		//KafkaConsumerClient_->RegisterCallback(Delegate);
		KafkaConsumerClient_->StartAsync(1000);
		return true;
	}
	return false;
}

bool USimpleKafkaFunctionLib::StartConsumerWithTopics(const FString& URL, TArray<FString> TopicsStr, const FString& GroudID)
{
	if (KafkaConsumerClient_) //单例
	{
		return false;
	}

	std::vector<std::string> vec;
	for (FString str : TopicsStr)
	{
		if (!str.IsEmpty())
		{
			vec.push_back(TCHAR_TO_UTF8(*str));
		}
	}
	KafkaConsumerClient_ = new SimpleKafkaConsumer(TCHAR_TO_UTF8(*URL), TCHAR_TO_UTF8(*GroudID), vec);
	if (KafkaConsumerClient_->Init())
	{
		KafkaConsumerClient_->StartAsync(1000);
		return true;
	}
	return false;
}

void USimpleKafkaFunctionLib::SetPauseConsumer(bool Pause)
{
	if (KafkaConsumerClient_)
	{
		KafkaConsumerClient_->SetPauseAsyc(Pause);
	}
}

void USimpleKafkaFunctionLib::StopConsumer()
{
	if (KafkaConsumerClient_)
	{
		KafkaConsumerClient_->Stop();
		KafkaConsumerClient_ = nullptr;
	}
}

void USimpleKafkaFunctionLib::RegisterConsumerCallback(FKafkaConsumerCallback_BP Callback)
{
	RegisterConsumerCallback(KafkaConsumerClient_, Callback);
}


SimpleKafkaConsumer* USimpleKafkaFunctionLib::CreateConsumerWithTopic(const FString& URL, const FString& TopicStr, const FString& GroupID)
{
	SimpleKafkaConsumer* Client_ = new SimpleKafkaConsumer(TCHAR_TO_UTF8(*URL), TCHAR_TO_UTF8(*GroupID), TCHAR_TO_UTF8(*TopicStr));
	if (Client_->Init())
	{
		Client_->StartAsync(1000);
	}
	return Client_;
}

void USimpleKafkaFunctionLib::RegisterConsumerCallback(SimpleKafkaConsumer* Consumer, FKafkaConsumerCallback_BP Callback)
{
	if (Consumer)
	{
		Consumer->GetCallbackHandle()->BindLambda([Callback](FString TopicName, FString msg)
		{
			Callback.ExecuteIfBound(TopicName,msg);
		});
	}
}

void USimpleKafkaFunctionLib::StopConsumer(SimpleKafkaConsumer* Consumer)
{
	if (Consumer)
	{
		Consumer->Stop();
	}
}


void USimpleKafkaFunctionLib::TestConsumer()
{
	//KafkaConsumerClient_ = new SimpleKafkaConsumer("127.0.0.1:9092","0", "Test");
	KafkaConsumerClient_ = new SimpleKafkaConsumer("127.0.0.1:9092", "0", "test");
	if (KafkaConsumerClient_->Init()) {

		//KafkaConsumerClient_->Start(1000);

		KafkaConsumerClient_->StartAsync(1000);
	}
}




///////////////////////////////////////////////////////////////////////////
bool USimpleKafkaFunctionLib::StartProducer(const FString& URL, const FString& TopicStr, int32 Partion)
{
	if (KafkaProducer)
	{
		return false;
	}

	KafkaProducer = new SimpleKafkaProducer(TCHAR_TO_UTF8(*URL), TCHAR_TO_UTF8(*TopicStr), Partion);
	return KafkaProducer->Init();
}

bool USimpleKafkaFunctionLib::SendMessag(const FString& Msg, const FString& KeyStr)
{
	if (KafkaProducer)
	{
		return KafkaProducer->SendMessage(TCHAR_TO_UTF8(*Msg), TCHAR_TO_UTF8(*KeyStr));
	}
	return false;
}

void USimpleKafkaFunctionLib::StopProducer()
{
	if (KafkaProducer)
	{
		KafkaProducer = nullptr;
	}
}

void USimpleKafkaFunctionLib::TestProduer()
{
	KafkaProducer = new SimpleKafkaProducer("127.0.0.1:9092","test",0);
	KafkaProducer->Init();
	KafkaProducer->SendMessage("test hello world", "");
}
