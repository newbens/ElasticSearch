package com.niuben;

import com.alibaba.fastjson.JSON;
import com.niuben.pojo.User;
import net.minidev.json.JSONArray;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;

@SpringBootTest
class EsApiApplicationTests {

    @Autowired
    RestHighLevelClient restHighLevelClient;

    @Test
    void testCreateIndex() throws IOException {
        //1.创建索引请求。
        CreateIndexRequest request = new CreateIndexRequest("niuben_index2");
        //2.执行请求,获得响应
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("name")
                .field("type", "keyword")
                .endObject()
                .startObject("age")
                .field("type", "integer")
                .endObject()
                .endObject()
                .endObject();
        request.mapping(builder);
        CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    //测试获取索引
    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("niuben_index2");
        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //测试删除索引
    @Test
    void testDelIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("niuben_index2");
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete);
    }

    //测试添加文档
    @Test
    void testAddDocument() throws IOException {
        //创建对象
        User user = new User("牛犇", 22);
        //获取请求
        IndexRequest niuben_index = new IndexRequest("niuben_index2");
        //设置规则
        niuben_index.id("1");
        niuben_index.timeout(TimeValue.timeValueSeconds(1));
        //将数据放入请求
        niuben_index.source(JSON.toJSONString(user), XContentType.JSON);
        //客户端发送请求,获取响应的结果
        IndexResponse res = restHighLevelClient.index(niuben_index, RequestOptions.DEFAULT);
        System.out.println(res.toString());
        System.out.println(res.status());

    }

    //获取文档是否存在
    void testIsExist() throws IOException {
        GetRequest niuben_index = new GetRequest("niuben_index", "1");
        System.out.println(restHighLevelClient.exists(niuben_index, RequestOptions.DEFAULT));
    }

    //获取文档信息
    @Test
    void testGetDocument() throws IOException {
        GetRequest request = new GetRequest("niuben_index2", "1");
        GetResponse res = restHighLevelClient.get(request, RequestOptions.DEFAULT);
        System.out.println(res.getSourceAsString());
    }

    //更新
    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("niuben_index", "1");
        User user = new User("张三", 55);
        updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);
        UpdateResponse response = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(response.status());
    }

    //删除文档记录
    @Test
    void testDelDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("niuben_index", "1");
        DeleteResponse delete = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.status());
    }

    //批量插入
    @Test
    void testBulkInsert() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");
        ArrayList<User> userList = new ArrayList<>();
        userList.add(new User("王一", 18));
        userList.add(new User("赵二", 19));
        userList.add(new User("张三", 20));
        userList.add(new User("李四", 21));
        //批处理请求
        for (int i = 0; i < userList.size(); i++) {
            bulkRequest.add(new IndexRequest("niuben_index")
                    .id(i + 1 + "")
                    .source(JSON.toJSONString(userList.get(i)), XContentType.JSON));
        }
        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.status());
    }

    //查询
    @Test
    void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("niuben_index2");
        //构建搜索条件
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchQuery("name", "牛犇"));
        builder.timeout(new TimeValue(12));
        searchRequest.source(builder);
        SearchResponse response = restHighLevelClient.search(searchRequest,RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(response.getHits()));
    }
}

