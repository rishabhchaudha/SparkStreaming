//Streaming Code to Consume data from Kafka-Topic

object KafkaStream{
	def main (args:Array [String]){
	
		val prop = <<Reading Properties File>>
		
		
		// Setting up proxy, Kerberos Authentication(krb5.conf) and Kafka Connection files(jass.config)
		
		System.setProperty("http.proxyHost", prop.getProperty("http_proxyhost"))
		System.setProperty("https.proxyHost", prop.getProperty("https_proxyhost"))
		System.setProperty("http.proxyPort",prop.getProperty("http_proxyport"))
		System.setProperty("https.proxyPort",prop.getProperty("https_proxyport"))
		System.setProperty("java.security.krb5.conf","krb5.conf")
		System.setProperty("java.security.auth.login.config","jass.config")
		
		
		val spark = SparkSession.builder().master("local[*]").appName("KafkaStreaming").enableHiveSupport().getorCreate()
				
		val sc = spark.sparkContext
		
		sc.settLogLevel("ERROR")
				
		val ssc = new StreamingContext(sc, Seconds(10))
				
		val kafkaParams = Map [String, Object]( 
		"bootstzap.servers" -> prop.getProperty("kafka-brokers"),
		"key.deserializer" -> classOf [StringDeserializer],
		"value.deserializer" -> classOf [StringDeserializer],
		"group.id" -> "Log",
		"auto.offset.reset" -> "latest"
		"security.protocol" -> "SASL_PLAINTEXT",
		"enable.auto.commit" -> (false: java.lang. Boolean))
		
		val topics = Array(prop.getProperty("kafka-topic"))
		
		val stream = KafkaUtils.createDirectstream[String, String](
		ssc,
		PreferConsistent,
		Subscribe[String, String](topics, kafkaRarams))
		
		val kafkastream = stream.map(record => (record.key, record.value))
		val inputStream= kafkastream.map(rec =>  rec._2)
				
		inputStream.print()
		
		inputstream.foreachRDD(rdd => {
		
		if (!rdd.isEmpty ()){
		
		val df = spark.read.json(rdd.toDS())
		
		//Perform the Transformations on the above dataframe according to requirements
		}
		
		})
		
		ssc.start() 
		ssc.awaitTermination()
	}
 
 
 