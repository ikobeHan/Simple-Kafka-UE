#pragma once

#include "Kismet/BlueprintFunctionLibrary.h"
#include "SimpleKafkaFunctionLib.generated.h"


DECLARE_DYNAMIC_DELEGATE_OneParam(FKafkaConsumerCallback_BP, FString, Msg);

UCLASS()
class SIMPLEKAFKA_API USimpleKafkaFunctionLib : public UBlueprintFunctionLibrary
{
	GENERATED_BODY()

public:
	/*
	* 暂时做出单独的消费者，约定只需要消费一个topic即可。
	*/
	UFUNCTION(BlueprintCallable, Category = "SimpleKafka|Consumer")
	static bool StartConsumer(const FString& URL, const FString& TopicStr, const FString& GroudID = "0");

	//游戏进程销毁之前调一下，销毁下
	UFUNCTION(BlueprintCallable, Category = "SimpleKafka|Consumer")
	static void StopConsumer();

	UFUNCTION(BlueprintCallable, Category = "SimpleKafka|Consumer")
	static void RegisterConsumerCallback(FKafkaConsumerCallback_BP Callback);


	/*
	* 生产者也单例吧，只需要生产一个给服务器的topic
	*/
	UFUNCTION(BlueprintCallable, Category = "SimpleKafka|Producer")
	static bool StartProducer(const FString& URL, const FString& TopicStr, int32 Partion = 0);

	UFUNCTION(BlueprintCallable, Category = "SimpleKafka|Producer")
	static bool SendMessag(const FString& Msg, const FString& KeyStr = "");

	//游戏进程销毁之前调一下，销毁下
	UFUNCTION(BlueprintCallable, Category = "SimpleKafka|Producer")
	static void StopProducer();


public:
	// ONLY for debug
	UFUNCTION(BlueprintCallable, Category = "SimpleKafka|Consumer")
	static void TestConsumer();

	// ONLY for debug
	UFUNCTION(BlueprintCallable, Category = "SimpleKafka|Producer")
	static void TestProduer();

private:
	static class SimpleKafkaConsumer* KafkaConsumerClient_;

	static class SimpleKafkaProducer* KafkaProducer;
};
