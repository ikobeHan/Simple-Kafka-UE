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
	if (KafkaConsumerClient_) //单例吧。只需要监听一个topic
	{
		KafkaConsumerClient_->GetCallbackHandle().BindLambda([Callback](FString msg) 
		{
			Callback.ExecuteIfBound(msg);
		});
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
