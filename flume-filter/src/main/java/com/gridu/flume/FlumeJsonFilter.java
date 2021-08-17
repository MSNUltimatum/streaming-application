package com.gridu.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

public class FlumeJsonFilter implements Interceptor, Interceptor.Builder {

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String eventBody = filterJson(new String(body));
        event.setBody(eventBody.getBytes());

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {
    }

    @Override
    public void initialize() {
    }

    private String filterJson(String jsonRaw) {
        if (jsonRaw.isEmpty()) {
            return jsonRaw;
        }

        char firstChar = jsonRaw.charAt(0);
        char lastChar = jsonRaw.charAt(jsonRaw.length() - 1);
        int firstIndex = 0;
        int lastIndex = jsonRaw.length();

        if (firstChar == '[')
            firstIndex++;

        if (lastChar == ']' || lastChar == ',')
            lastIndex--;
        return jsonRaw.substring(firstIndex, lastIndex);
    }

    @Override
    public Interceptor build() {
        return new FlumeJsonFilter();
    }

    @Override
    public void configure(Context context) {

    }
}
