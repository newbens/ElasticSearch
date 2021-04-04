package com.niuben.utils;

import com.niuben.pojo.Content;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HtmlParseUtil {
    public static void main(String[] args) throws IOException {
        List<Content> xyj = new HtmlParseUtil().parseJD("西游记");
        for (Content content : xyj) {
            System.out.println(content);
        }
    }

    public List<Content> parseJD(String keyword) throws IOException {
        //获得请求
        String url = "https://search.jd.com/Search?keyword="+keyword;
        //解析网页 ,jsoup返回的document对象就是js页面对象
        Document document = Jsoup.parse(new URL(url), 3000);
        Element list = document.getElementById("J_goodsList");
        Elements li = list.getElementsByTag("li");
        ArrayList<Content> goodList = new ArrayList<>();
        //获取li中的内容
        for (Element e : li) {
            String img = e.getElementsByTag("img").eq(0).attr("data-lazy-img");
            String price = e.getElementsByClass("p-price").eq(0).text();
            String title = e.getElementsByClass("p-name").eq(0).text();
            goodList.add(new Content(title, img, price));
        }
        return goodList;
    }
}
