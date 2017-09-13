package httpclients

import com.github.tomakehurst.wiremock.http.GenericHttpUriRequest
import com.github.tomakehurst.wiremock.junit.WireMockRule
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.config.RequestConfig
import org.apache.http.concurrent.FutureCallback
import org.apache.http.conn.DnsResolver
import org.apache.http.conn.HttpClientConnectionManager
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.impl.conn.SystemDefaultDnsResolver
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor
import org.apache.http.impl.nio.reactor.IOReactorConfig
import org.apache.http.nio.client.HttpAsyncClient
import org.apache.http.nio.reactor.ConnectingIOReactor
import org.junit.Rule
import spock.lang.Specification

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future

import static com.github.tomakehurst.wiremock.client.WireMock.*
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options

class ClientsSpec extends Specification {

    private final static int THREADS = 10

    @Rule
    WireMockRule wireMock = new WireMockRule(options().dynamicPort().containerThreads(THREADS + 5))

    ExecutorService executor = Executors.newFixedThreadPool(THREADS)

    def setup() {
        executor = Executors.newFixedThreadPool(THREADS)
    }

    private void stubWithDelay(int delay) {
        stubFor(get("/").willReturn(aResponse()
                .withFixedDelay(delay)
                .withBody("Hello JavaZone!")
        ))
    }





    def "let's try HttpUrlConnection connection pooling"() {
        given:
        int port = wireMock.port()

        stubWithDelay(1000)

        when:
        for (int i = 0; i < THREADS; ++i) {
            executor.submit({
                HttpURLConnection conn
                try {
                    conn = URI.create("http://localhost:$port/").toURL().openConnection()

                    println "Connecting..."

                    conn.connect()

                    println "Received response: ${conn.inputStream.text}"
                } finally {
                    conn?.inputStream?.close()
                }
            })
        }

        then:
        Thread.sleep(1050)
    }











    def "let's try Apache HTTP Client with default connection pool"() {
        given:
        int port = wireMock.port()

        stubWithDelay(1000)


        and:
        HttpClient client = HttpClientBuilder.create()
                .build()

        when:
        for (int i = 0; i < THREADS; ++i) {
            executor.submit({
                HttpResponse response
                try {
                    println "Connecting..."
                    response = client.execute(new GenericHttpUriRequest("GET", "http://localhost:$port/"))

                    println "Received response: ${response.entity.content.text}"
                } finally {
                    response?.entity.content.close()
                }
            })
        }

        then:
        Thread.sleep(1050)
    }













    def "let's try Apache HTTP Client with configured connection pool"() {
        given:
        int port = wireMock.port()

        stubWithDelay(1000)


        and:
        HttpClientConnectionManager manager = new PoolingHttpClientConnectionManager()
        manager.setDefaultMaxPerRoute(15)
        manager.setMaxTotal(200)

        HttpClient client = HttpClientBuilder.create()
                .setConnectionManager(manager)
                .build()

        when:
        for (int i = 0; i < THREADS; ++i) {
            executor.submit({
                HttpResponse response
                try {
                    println "Connecting..."
                    response = client.execute(new GenericHttpUriRequest("GET", "http://localhost:$port/"))

                    println "Received response: ${response.entity.content.text}"
                } finally {
                    response?.entity.content.close()
                }
            })
        }

        then:
        Thread.sleep(1050)
    }











    def "let's try Apache HTTP Client default timeouts"() {
        given:
        int port = wireMock.port()

        stubWithDelay(10000)


        and:
        HttpClient client = HttpClientBuilder.create()
                .build()

        when:
        HttpResponse response
        try {
            println "Connecting..."
            response = client.execute(new GenericHttpUriRequest("GET", "http://localhost:$port/"))

            println "Received response"
        } finally {
            response?.entity?.content?.close()
        }

        then:
        true
    }














    def "let's try Apache HTTP Client timeouts"() {
        given:
        int port = wireMock.port()

        stubWithDelay(20000)


        and:
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(1000)
                .build()

        HttpClient client = HttpClientBuilder.create()
                .setDefaultRequestConfig(requestConfig)
                .build()

        when:
        HttpResponse response
        long startTime = System.currentTimeMillis();
        try {
            println "Connecting..."
            response = client.execute(new GenericHttpUriRequest("GET", "http://localhost:$port/"))

            println "Received response!"
        } finally {
            println "Request took: ${System.currentTimeMillis() - startTime}"
            response?.entity?.content?.close()
        }

        then:
        true
    }










    def "let's try Apache Async Client timeouts"() {
        given:
        int port = wireMock.port()

        stubWithDelay(20000)


        and:
        IOReactorConfig reactorConfig = IOReactorConfig.custom()
                .setSoTimeout(100)
                .build()

        ConnectingIOReactor reactor = new DefaultConnectingIOReactor(
                reactorConfig,
                new ThreadFactoryBuilder().setNameFormat("http-client-%d").build()
        )


        PoolingNHttpClientConnectionManager manager = new PoolingNHttpClientConnectionManager(reactor);

        HttpAsyncClient asyncClient = HttpAsyncClientBuilder.create()
                .setConnectionManager(manager)
                .build();
        asyncClient.start()

        when:
        HttpResponse response
        long startTime = System.currentTimeMillis();
        try {
            println "Connecting..."
            Future<HttpResponse> r = asyncClient.execute(new GenericHttpUriRequest("GET", "http://localhost:$port/"), futureCallback())
            r.get()
        } finally {
            println "Request took: ${System.currentTimeMillis() - startTime}"
            response?.entity?.content?.close()
        }

        then:
        true
    }






    def "let's try Apache Async Client timeouts with custom polling"() {
        given:
        int port = wireMock.port()

        stubWithDelay(20000)


        and:
        IOReactorConfig reactorConfig = IOReactorConfig.custom()
                .setSoTimeout(100)
                .setSelectInterval(50)
                .build()

        ConnectingIOReactor reactor = new DefaultConnectingIOReactor(
                reactorConfig,
                new ThreadFactoryBuilder().setNameFormat("http-client-%d").build()
        )


        PoolingNHttpClientConnectionManager manager = new PoolingNHttpClientConnectionManager(reactor);

        HttpAsyncClient asyncClient = HttpAsyncClientBuilder.create()
                .setConnectionManager(manager)
                .build();
        asyncClient.start()

        when:
        HttpResponse response
        long startTime = System.currentTimeMillis();
        try {
            println "Connecting..."
            Future<HttpResponse> r = asyncClient.execute(new GenericHttpUriRequest("GET", "http://localhost:$port/"), futureCallback())
            r.get()
        } finally {
            println "Request took: ${System.currentTimeMillis() - startTime}"
            response?.entity?.content?.close()
        }

        then:
        true
    }

    def "let's try Apache Async Client DNS resolution"() {
        given:
        int port = wireMock.port()

        stubWithDelay(20000)


        and:
        IOReactorConfig reactorConfig = IOReactorConfig.custom()
                .setSoTimeout(100)
                .setSelectInterval(50)
                .build()

        ConnectingIOReactor reactor = new DefaultConnectingIOReactor(
                reactorConfig,
                new ThreadFactoryBuilder().setNameFormat("http-client-%d").build()
        )


        PoolingNHttpClientConnectionManager manager = new PoolingNHttpClientConnectionManager(
                reactor, null,
                new BlockingDnsResolver()
        );

        HttpAsyncClient asyncClient = HttpAsyncClientBuilder.create()
                .setConnectionManager(manager)
                .build();
        asyncClient.start()

        when:
        HttpResponse response
        long startTime = System.currentTimeMillis();
        try {
            println "Connecting..."
            Future<HttpResponse> r = asyncClient.execute(new GenericHttpUriRequest("GET", "http://localhost:$port/"), futureCallback())
            println "This message should be printed before we block"
            r.get()
        } finally {
            println "Request took: ${System.currentTimeMillis() - startTime}"
            response?.entity?.content?.close()
        }

        then:
        true
    }


    private FutureCallback<HttpResponse> futureCallback() {
        return new FutureCallback<HttpResponse>() {
            @Override
            void completed(HttpResponse result) {

            }

            @Override
            void failed(Exception ex) {

            }

            @Override
            void cancelled() {

            }
        }
    }

    private static class BlockingDnsResolver implements DnsResolver {

        @Override
        InetAddress[] resolve(String host) throws UnknownHostException {
            Thread.sleep(10000);
            return SystemDefaultDnsResolver.INSTANCE.resolve(host);
        }
    }
}
