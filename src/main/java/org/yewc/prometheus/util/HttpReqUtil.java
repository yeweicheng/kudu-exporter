package org.yewc.prometheus.util;

import com.squareup.okhttp.*;

import java.io.IOException;

public class HttpReqUtil {

    private static final OkHttpClient client = new OkHttpClient();

    public static String get(String url) throws IOException {
        return get(url, false);
    }

    public static String get(String url, boolean ignoreError) throws IOException {
        Request request = new Request.Builder().url(url)
                .build();
        Response response = client.newCall(request).execute();
        if (!response.isSuccessful() && !ignoreError)
            throw new IOException("请求有错：" + response);
        return response.body().string();
    }
}

