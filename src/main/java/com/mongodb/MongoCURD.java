package com.mongodb;

import java.util.HashMap;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.Test;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

/**
 * 
 * @author Administrator
 * MongoDB 增删改查操作
 *
 */
public class MongoCURD {

	// 查询多个文档
	@Test
	public void findAll(){
		// 创建链接的客户端
		MongoClient client = new MongoClient("127.0.0.1", 27017);
		// 获取数据库对象
		MongoDatabase db = client.getDatabase("mydata");
		// 获取操作的集合对象（表）
		MongoCollection<Document> collection =  db.getCollection("user");
		
		// 集体操作
		FindIterable<Document> it = collection.find();
		MongoCursor<Document> cursor = it.iterator();
		
		while(cursor.hasNext()){
			
			Document test = cursor.next();
			String name = test.getString("name");
			String gender = test.getString("gender");
			System.out.println("name："+name+", 性别："+gender);
			
		}
		
		// 释放资源
		cursor.close();
		client.close();
		
	}
	
	// 查询单个文档
	@Test
	public void findOne(){
		// 创建链接的客户端
		MongoClient client = new MongoClient("127.0.0.1", 27017);
		// 获取数据库对象
		MongoDatabase db = client.getDatabase("mydata");
		// 获取操作的集合对象（表）
		MongoCollection<Document> collection =  db.getCollection("user");
		
		// 查询条件
		Map map  = new HashMap();
		// map.put("_id", new ObjectId("57bf0f5683529d7b36b6c429"));
		Bson filter = new BasicDBObject("name","name_a");
		
		// 集体操作
		FindIterable<Document> it = collection.find(filter);
		MongoCursor<Document> cursor = it.iterator();
		
		while(cursor.hasNext()){
			
			Document test = cursor.next();
			String name = test.getString("name");
			String gender = test.getString("gender");
			System.out.println("name："+name+", 性别："+gender);
			
		}
		
		// 释放资源
		cursor.close();
		client.close();
	}
	
	// 插入单个文档
	@Test
	public void insertOne(){
		// 创建链接的客户端
			MongoClient client = new MongoClient("127.0.0.1", 27017);
			// 获取数据库对象
			MongoDatabase db = client.getDatabase("mydata");
			// 获取操作的集合对象（表）
			MongoCollection<Document> collection =  db.getCollection("user");
			
			// 插入
			Map map = new HashMap();
			map.put("name", "name_d");
			map.put("gender", "man");
			
			Map hobbyMap = new HashMap();
			hobbyMap.put("lan", "java");
			hobbyMap.put("person", "KongFu");
			
			map.put("hobby", hobbyMap );
			
			Document doc = new Document(map);
			collection.insertOne(doc);
			
			// 释放资源
			client.close();
	}
	
	// 更新单个文档
	@Test
	public void updateOne(){
		// 创建链接的客户端
		MongoClient client = new MongoClient("127.0.0.1", 27017);
		// 获取数据库对象
		MongoDatabase db = client.getDatabase("mydata");
		// 获取操作的集合对象（表）
		MongoCollection<Document> collection =  db.getCollection("user");
		
		// 条件
		Bson filter = new BasicDBObject("name", "name_d");
		
		// 封装需要更新的数据
		Map map = new HashMap();
		map.put("name", "name_d_1");
		
		Map hobbyMap = new HashMap();
		hobbyMap.put("lan", "java_1");
		hobbyMap.put("person", "KongFu_1");
		
		map.put("hobby", hobbyMap);
		
		Bson update = new BasicDBObject(map);
		
		collection.updateMany(filter, new BasicDBObject("$set", update));
		
		// 释放资源
		client.close();
	}
	
	// 删除单个文档
	@Test
	public void deleteOne(){
		// 创建链接的客户端
		MongoClient client = new MongoClient("127.0.0.1", 27017);
		// 获取数据库对象
		MongoDatabase db = client.getDatabase("mydata");
		// 获取操作的集合对象（表）
		MongoCollection<Document> collection =  db.getCollection("user");
		
		Map map = new HashMap();
		map.put("name", "name_d_1");
		
		collection.deleteMany(new BasicDBObject(map));
		
		// 释放资源
		client.close();
	}
}
