package com.niuben.service;

import com.alibaba.fastjson.JSON;
import com.niuben.pojo.Content;
import com.niuben.utils.HtmlParseUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class ContentService {

    @Autowired
    private RestHighLevelClient client;
    //1解析数据放入es
    public Boolean  parseContent(String keyword) throws IOException {
        List<Content> contents = new HtmlParseUtil().parseJD(keyword);//获取内容
        //入库
        BulkRequest bk = new BulkRequest() //获取批量处理请求
                .timeout("2m");//设置超时时间
        for (Content content : contents) {
            bk.add(
                    new IndexRequest("jd_goods")//设置存入索引
                    .source(JSON.toJSONString(content), XContentType.JSON)//转换为Json格式
            );
        }
        BulkResponse bulkResponse = client.bulk(bk, RequestOptions.DEFAULT);//执行存入
        return !bulkResponse.hasFailures();
    }
    public List<Map<String, Object>> searchPage(String keyword, int pageNo, int pageSize) throws IOException {
        if(pageNo < 1) pageNo = 1;
        SearchRequest searchRequest = new SearchRequest("jd_goods");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //分页
        sourceBuilder.from(pageNo);
        sourceBuilder.size(pageSize);
        //精准匹配
        TermQueryBuilder termQuery= QueryBuilders.termQuery("title", keyword);
        sourceBuilder.query(termQuery);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        //构建高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        //设置那里高亮
        highlightBuilder.field("title");
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");
        sourceBuilder.highlighter(highlightBuilder);
        //执行搜索
        searchRequest.source(sourceBuilder);
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        //解析结果
        List<Map<String, Object>> list = new ArrayList<>();
        for (SearchHit hit : search.getHits().getHits()) {
            //解析高亮字段
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField title = highlightFields.get("title");
            //原来的结果
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            //替换
            if (title != null) {
                Text[] t = title.fragments();//获取title所有
                String highLightTitle = "";
                for (Text text : t) {
                    highLightTitle += text;
                }
                sourceAsMap.put("title", highLightTitle);
            }
            list.add(hit.getSourceAsMap());
        }
        if (list.size() == 0) {
            parseContent(keyword);
            return searchPage(keyword, pageNo, pageSize);
        } else return list;
    }
}
