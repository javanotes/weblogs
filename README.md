# weblogs
A searchable 'big' logging infrastructure.


Logging is a general cross cutting concern with almost all applications. We have robust libraries like log4j/logback or the jdk logging present for it. For many projects the logs are stored in some database for analysis afterwards.

So it is a persistent, searchable, scalable logging infrastructure that can be customized, or simply used as a plugin to extend the existing logging framework of an application. We will discuss it as a Log4j plugin, using a custom Log4j appender to utilize the framework.


The project uses Stratio secondary index plugin for Cassandra; a Lucene based full text search implementation on Cassandra. The core project is developed as a Spring boot application that can run as an embedded servlet container, or deployed as webapp. It provides:
<ul>
  <li>A RESTful api for log request ingestion. For e.g, the Log4j appender would POST logging requests to the api</li>
  <li>A web based console for viewing and searching through logs, and some data visualization</li>
</ul>

More details can be found <a href="http://hotjavanotes.blogspot.in/">here</a>
