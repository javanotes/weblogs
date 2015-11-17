/* ============================================================================
*
* FILE: IngestionControllerTest.java
*
* MODULE DESCRIPTION:
* See class description
*
* Copyright (C) 2015 by
* ERICSSON
*
* The program may be used and/or copied only with the written
* permission from Ericsson Inc, or in accordance with
* the terms and conditions stipulated in the agreement/contract
* under which the program has been supplied.
*
* All rights reserved
*
* ============================================================================
*/
package com.ericsson.weblogs;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.data.cassandra.repository.MapId;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import com.ericsson.weblogs.controllers.WebServicesController;
import com.ericsson.weblogs.dao.LogEventRepository;
import com.ericsson.weblogs.dto.LogIngestionStatus;
import com.ericsson.weblogs.dto.LogRequest;
import com.ericsson.weblogs.dto.LogRequests;
import com.ericsson.weblogs.dto.LogResponse;
import com.fasterxml.jackson.databind.ObjectMapper;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
@WebAppConfiguration
public class IngestionControllerTest {

  final int batchSize = 10;
  final String appId = "applicationId", bucket = "bucket";
  
  @Autowired
  private WebApplicationContext webApplicationContext;
  
  private MockMvc mvc;
  @Autowired
  private LogEventRepository repo;
  private LogRequest event;
  @Before
  public void setup()
  {
    mvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
  }
  @After
  public void delete()
  {
    if(event != null)
    {
      try {
        MapId id = event.toMapId();
        repo.delete(id);
      } catch (Exception e) {
        //Assert.fail(e.getMessage());
      }
    }
  }
  @Test
  public void testIngestRequest()
  {
    event = new LogRequest();
    event.setLogText("This is some bla blaah bla logging at info level");
    event.setApplicationId(appId);
    
    
    try 
    {
      String json = new ObjectMapper().writer().writeValueAsString(event);
      MvcResult result = mvc.perform(MockMvcRequestBuilders.post("/services/ingest").contentType(MediaType.APPLICATION_JSON).content(json))
      .andDo(MockMvcResultHandlers.print())
      .andReturn();
      
      Assert.assertEquals(HttpStatus.OK.value(), result.getResponse().getStatus());
      LogResponse lr = new ObjectMapper().reader(LogResponse.class).readValue(result.getResponse().getContentAsString());
      Assert.assertEquals(LogIngestionStatus.SUCCESS, lr.getStatus());
      
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
  
  @Test
  public void testIngestBatchRequest()
  {
    event = new LogRequest();
    event.setLogText("This is some bla blaah bla logging at info level");
    event.setApplicationId(appId);
    
    List<LogRequest> requests = new ArrayList<>(batchSize);
    LogRequest l;
    for(int i=0; i<batchSize; i++)
    {
      l = new LogRequest();
      l.setLogText("This is some bla blaah bla logging at info level");
      l.setApplicationId(appId);
      
      requests.add(l);
    }
        
    try 
    {
      String json = new ObjectMapper().writer().writeValueAsString(new LogRequests(requests));
      MvcResult result = mvc.perform(MockMvcRequestBuilders.post("/services/ingestbatch").contentType(MediaType.APPLICATION_JSON).content(json))
      .andDo(MockMvcResultHandlers.print())
      .andReturn();
      
      Assert.assertEquals(HttpStatus.OK.value(), result.getResponse().getStatus());
      LogResponse lr = new ObjectMapper().reader(LogResponse.class).readValue(result.getResponse().getContentAsString());
      Assert.assertEquals(LogIngestionStatus.SUCCESS, lr.getStatus());
      
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
  
  @Test
  public void testIngestBatchRequestMandatoryMissing()
  {
    event = new LogRequest();
    event.setLogText("This is some bla blaah bla logging at info level");
    event.setApplicationId(appId);
    
    List<LogRequest> requests = new ArrayList<>(batchSize);
    LogRequest l;
    for(int i=0; i<batchSize; i++)
    {
      l = new LogRequest();
      l.setLogText("This is some bla blaah bla logging at info level");
      
      requests.add(l);
    }
        
    try 
    {
      String json = new ObjectMapper().writer().writeValueAsString(new LogRequests(requests));
      MvcResult result = mvc.perform(MockMvcRequestBuilders.post("/services/ingestbatch").contentType(MediaType.APPLICATION_JSON).content(json))
      .andDo(MockMvcResultHandlers.print())
      .andReturn();
      
      Assert.assertEquals(HttpStatus.OK.value(), result.getResponse().getStatus());
      LogResponse lr = new ObjectMapper().reader(LogResponse.class).readValue(result.getResponse().getContentAsString());
      Assert.assertEquals(LogIngestionStatus.REJECTED, lr.getStatus());
      Assert.assertTrue(lr.getMessage().startsWith(WebServicesController.RESP_MSG_VALIDATION_ERR));
      
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
  
  @Test
  public void testIngestRequestMandatoryMissing()
  {
    event = new LogRequest();
    event.setLogText("This is some bla blaah bla logging at info level");
    
    
    try 
    {
      String json = new ObjectMapper().writer().writeValueAsString(event);
      MvcResult result = mvc.perform(MockMvcRequestBuilders.post("/services/ingest").contentType(MediaType.APPLICATION_JSON).content(json))
      .andDo(MockMvcResultHandlers.print())
      .andReturn();
      
      Assert.assertEquals(HttpStatus.OK.value(), result.getResponse().getStatus());
      LogResponse lr = new ObjectMapper().reader(LogResponse.class).readValue(result.getResponse().getContentAsString());
      Assert.assertEquals(LogIngestionStatus.REJECTED, lr.getStatus());
      Assert.assertTrue(lr.getMessage().startsWith(WebServicesController.RESP_MSG_VALIDATION_ERR));
      
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
  
  @Test
  public void testIngestBatchRequestInvalidJson()
  {
    event = new LogRequest();
    event.setLogText("This is some bla blaah bla logging at info level");
    event.setApplicationId(appId);
    
    List<String> requests = new ArrayList<>(batchSize);
    for(int i=0; i<batchSize; i++)
    {
     requests.add("This is some bla blaah bla logging at info level");
    }
        
    try 
    {
      String json = new ObjectMapper().writer().writeValueAsString(requests);
      MvcResult result = mvc.perform(MockMvcRequestBuilders.post("/services/ingestbatch").contentType(MediaType.APPLICATION_JSON).content(json))
      .andDo(MockMvcResultHandlers.print())
      .andReturn();
      
      Assert.assertEquals(HttpStatus.OK.value(), result.getResponse().getStatus());
      LogResponse lr = new ObjectMapper().reader(LogResponse.class).readValue(result.getResponse().getContentAsString());
      Assert.assertEquals(LogIngestionStatus.REJECTED, lr.getStatus());
      Assert.assertEquals(WebServicesController.RESP_MSG_INV_JSON_FORMAT, lr.getMessage());
      
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
  @Test
  public void testIngestRequestInvalidJson()
  {
        
    try 
    {
      String json = new ObjectMapper().writer().writeValueAsString("This is a log");
      MvcResult result = mvc.perform(MockMvcRequestBuilders.post("/services/ingest").contentType(MediaType.APPLICATION_JSON).content(json))
      .andDo(MockMvcResultHandlers.print())
      .andReturn();
      
      Assert.assertEquals(HttpStatus.OK.value(), result.getResponse().getStatus());
      LogResponse lr = new ObjectMapper().reader(LogResponse.class).readValue(result.getResponse().getContentAsString());
      Assert.assertEquals(LogIngestionStatus.REJECTED, lr.getStatus());
      Assert.assertEquals(WebServicesController.RESP_MSG_INV_JSON_FORMAT, lr.getMessage());
      
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
}
