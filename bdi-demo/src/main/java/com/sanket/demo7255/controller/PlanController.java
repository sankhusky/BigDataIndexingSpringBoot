package com.sanket.demo7255.controller;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.sanket.demo7255.Demo7255Application;
import com.sanket.demo7255.exception.BadRequestException;
import com.sanket.demo7255.exception.NotAuthorisedException;
import com.sanket.demo7255.exception.PreConditionFailedException;
import com.sanket.demo7255.exception.ResourceNotFoundException;
import com.sanket.demo7255.security.JWTToken;
import com.sanket.demo7255.service.PlanService;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;


@RestController
public class PlanController {

    static int count = 0;

    @Autowired
    private RabbitTemplate template;
        

    PlanService planService = new PlanService();

    Map<String, String> cacheMap = new HashMap<String, String>();
    static Map<String,String> idMap = new HashMap<>();
    static {
    	idMap.put("1234520xvc30asdf-502", "linkedService_1234520xvc30asdf-502");
    	idMap.put("1234520xvc30sfs-505", "linkedService_1234520xvc30sfs-505");
    	idMap.put("1234vxc2324sdf-501", "planCostShares_1234vxc2324sdf-501");
    	idMap.put("12xvxc345ssdsds-508", "plan_12xvxc345ssdsds-508");
    	idMap.put("1234512xvc1314asdfs-503", "planserviceCostShares_1234512xvc1314asdfs-503");
    	idMap.put("1234512xvc1314sdfsd-506", "planserviceCostShares_1234512xvc1314sdfsd-506");
    	idMap.put("27283xvx9asdff-504", "planservice_27283xvx9asdff-504");
    	idMap.put("27283xvx9sdf-507", " planservice_27283xvx9sdf-507");
    }
    JWTToken jwtToken = new JWTToken();

//    @RequestMapping(method = RequestMethod.GET, value = "/test")
//    public String index() {
//        PlanController.count++;
//
//        Map<String, String> actionMap = new HashMap<>();
//        actionMap.put("operation", "get");
//        actionMap.put("uri", "http://localhost:9200/testindex/_doc/" + PlanController.count);
//        String body = "{\"message\": \"test\", \"value\": 0 }";
//        actionMap.put("body", body);
//
//        System.out.println("Sending message: " + actionMap);
//
//        template.convertAndSend(BdiDemoApplication.MESSAGE_QUEUE, actionMap);
//
//        return Integer.toString(PlanController.count);
//    }

    @ResponseStatus(value = HttpStatus.CREATED)
    @RequestMapping(method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE, value = "/token")
    public String generateToken() {
        String token;
        try {
            token = jwtToken.generateToken();
        } catch (Exception e) {
            throw new BadRequestException(e.getMessage());
        }

        return "{\"token\": \"" + token + "\"}";
    }

    @ResponseStatus(value = HttpStatus.CREATED)
    @RequestMapping(method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan")
    public String savePlan(@RequestBody String json, HttpServletRequest request, HttpServletResponse response) {

//        // Authorization
//        String token = request.getHeader("Authorization").replace("Bearer ", "");
//        if (!this.jwtToken.isTokenValid(token)){
//            throw new NotAuthorisedException("The token is not valid");
//        }

        JSONObject plan = new JSONObject(json);

        // Validate the plan against the schema
        JSONObject plan_schema = new JSONObject(new JSONTokener(PlanController.class.getResourceAsStream("/plan-schema.json")));

        Schema schema = SchemaLoader.load(plan_schema);
        try {
            schema.validate(plan);
        } catch (ValidationException e) {
            throw new BadRequestException(e.getMessage());
        }

        String objectKey = this.planService.savePlan(plan, (String)plan.get("objectType"));

        //break into objects and send to queue
        sendToIndex(new JSONObject(json), "");

        // cache the objectId
        this.cacheMap.put(objectKey, String.valueOf(plan.hashCode()));
        response.setHeader(HttpHeaders.ETAG, String.valueOf(plan.hashCode()));

        return "{\"objectId\": \"" + objectKey + "\"}";
    }

    @ResponseStatus(value = HttpStatus.OK)
    @RequestMapping(method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan/{objectId}")
    public ResponseEntity<String> getPlan(@PathVariable String objectId, HttpServletRequest request, HttpServletResponse response) {

        // Authorization
        String token = request.getHeader("Authorization").replace("Bearer ", "");
        if (!this.jwtToken.isTokenValid(token)){
            throw new NotAuthorisedException("The token is not valid");
        }

        String if_none_match = request.getHeader(HttpHeaders.IF_NONE_MATCH);
        if (this.cacheMap.get(objectId) != null && this.cacheMap.get(objectId).equals(if_none_match)) {
            // etag matches, send 304
            return new ResponseEntity<String>(HttpStatus.NOT_MODIFIED);
        }

        JedisPool jedisPool = new JedisPool();
        Jedis jedis = jedisPool.getResource();
        JSONObject json = this.planService.getPlan(objectId);
        if (json == null) {
            throw new ResourceNotFoundException("Plan not found");
        }

        // cache the objectId
        this.cacheMap.put(objectId, String.valueOf(json.hashCode()));
        response.setHeader(HttpHeaders.ETAG, String.valueOf(json.hashCode()));

        return ResponseEntity.ok().body(json.toString());
    }

    @ResponseStatus(value = HttpStatus.OK)
    @RequestMapping(method = RequestMethod.PUT, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan/{objectId}")
    public ResponseEntity<String> updatePlan(@RequestBody String json, @PathVariable String objectId, HttpServletRequest request, HttpServletResponse response) {
        // Authorization
        String token = request.getHeader("Authorization").replace("Bearer ", "");
        if (!this.jwtToken.isTokenValid(token)){
            throw new NotAuthorisedException("The token is not valid");
        }

        //check etag
        String if_match = request.getHeader(HttpHeaders.IF_MATCH);
        if (if_match == null || if_match.isEmpty()) {
            // etag not provided throw 404
            throw new ResourceNotFoundException("etag not provided in request");
        }
        if (this.cacheMap.get(objectId) != null && !this.cacheMap.get(objectId).equals(if_match)) {
            // hash found in cache but does not match with etag
            throw new PreConditionFailedException("etag in request does not match hash in cache");
        }

        JSONObject plan = new JSONObject(json);

        //validate resource
        JSONObject plan_schema = new JSONObject(new JSONTokener(PlanController.class.getResourceAsStream("/plan-schema.json")));
        Schema schema = SchemaLoader.load(plan_schema);
        try {
            schema.validate(plan);
        } catch (ValidationException e) {
            throw new BadRequestException(e.getMessage());
        }

        //update resource
        String objectKey = this.planService.updatePlan(plan, (String)plan.get("objectType"));
        if (objectKey == null) {
            throw new BadRequestException("Update failed!");
        }

        // index object
        Map<String, String> actionMap = new HashMap<>();
        actionMap.put("operation", "SAVE");
        actionMap.put("uri", "http://localhost:9200");
        actionMap.put("index", "planindex");
        actionMap.put("body", json);

        System.out.println("Sending message: " + actionMap);

        template.convertAndSend(Demo7255Application.MESSAGE_QUEUE, actionMap);

        //update etag
        this.cacheMap.put(objectKey, String.valueOf(plan.hashCode()));
        response.setHeader(HttpHeaders.ETAG, String.valueOf(plan.hashCode()));

        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @ResponseStatus(value = HttpStatus.OK)
    @RequestMapping(method = RequestMethod.PATCH, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan/{objectId}")
    public ResponseEntity<String> patchPlan(@RequestBody String json, @PathVariable String objectId, HttpServletRequest request, HttpServletResponse response) {

        //authorization
        String token = request.getHeader("Authorization").replace("Bearer ", "");
        if (!this.jwtToken.isTokenValid(token)){
            throw new NotAuthorisedException("The token is not valid");
        }

        //check etag
        String if_match = request.getHeader(HttpHeaders.IF_MATCH);
        if (if_match == null || if_match.isEmpty()) {
            // etag not provided throw 404
            throw new ResourceNotFoundException("etag not provided in request");
        }
        if (this.cacheMap.get(objectId) != null && !this.cacheMap.get(objectId).equals(if_match)) {
            // hash found in cache but does not match with etag
            throw new PreConditionFailedException("etag in request does not match hash in cache");
        }

        JSONObject plan = new JSONObject(json);

        //merge json with saved value
        JSONObject mergedJson = this.planService.mergeJson(plan, idMap.get(objectId));
        if (mergedJson == null) {
            throw new ResourceNotFoundException("Resource not found");
        }

        //validate json
//        JSONObject plan_schema = new JSONObject(new JSONTokener(PlanController.class.getResourceAsStream("/plan-schema.json")));
//        Schema schema = SchemaLoader.load(plan_schema);
//        try {
//            schema.validate(mergedJson);
//        } catch (ValidationException e) {
//            throw new BadRequestException(e.getMessage());
//        }

        JSONObject planToIndex = new JSONObject(mergedJson.toString());

        //update json
        String objectKey = this.planService.updatePlan(mergedJson, (String)mergedJson.get("objectType"));
        if (objectKey == null) {
            throw new BadRequestException("Update failed!");
        }

//        // index object
//        Map<String, String> actionMap = new HashMap<>();
//        actionMap.put("operation", "SAVE");
//        actionMap.put("uri", "http://localhost:9200");
//        actionMap.put("index", "planindex");
//        actionMap.put("body", planToIndex.toString());
//
//        System.out.println("Sending message: " + actionMap);
//
////        template.convertAndSend(Demo7255Application.MESSAGE_QUEUE, actionMap);
//        template.convertAndSend(Demo7255Application.topicExchange, "sanket.indexing.queue", actionMap);

        sendToIndex(planToIndex, "");
        //update etag
        this.cacheMap.put(objectKey, String.valueOf(mergedJson.hashCode()));
        response.setHeader(HttpHeaders.ETAG, String.valueOf(mergedJson.hashCode()));

        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    @RequestMapping(method = RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan/{objectId}")
    public ResponseEntity<String> deletePlan(@PathVariable String objectId, HttpServletRequest request, HttpServletResponse response) {
//
//        // Authorization
//        String token = request.getHeader("Authorization").replace("Bearer ", "");
//        if (!this.jwtToken.isTokenValid(token)){
//            throw new NotAuthorisedException("The token is not valid");
//        }

        String if_match = request.getHeader(HttpHeaders.IF_MATCH);
        if (if_match == null || if_match.isEmpty()) {
            // etag not provided throw 404
            throw new ResourceNotFoundException("etag not provided in request");
        }
        if (this.cacheMap.get(objectId) != null && !this.cacheMap.get(objectId).equals(if_match)) {
            // hash found in cache but does not match with etag
            throw new PreConditionFailedException("etag in request does not match hash in cache");
        } else {
        	if(!objectId.contains("_")) {
        		objectId = "plan_"+objectId;
        	}
            if (!this.planService.deletePlan(objectId)) {
                // deletion failed
                throw new ResourceNotFoundException("Plan not found");
            }

            String indexObjectId = objectId.split("_")[1];
            System.out.println("All Keys:"+this.planService.allKeys);
            Set<String> keySet = new HashSet<>();
            for (String key : this.planService.allKeys) {
            	String mainKey = key.contains("_") ? key.split("_")[1] : key;
				if(!keySet.contains(mainKey)) {
					keySet.add(mainKey);
				}
			}
            // index object
            Map<String, String> actionMap = new HashMap<>();
            actionMap.put("operation", "DELETE");
            actionMap.put("uri", "http://localhost:9200");
            actionMap.put("index", "planindex");
            
            for (String deleteKey : keySet) {
            	actionMap.put("body", "{ \"objectId\" : \"" + deleteKey + "\" }");
            	
            	System.out.println("Sending message: " + actionMap);
            	
//            template.convertAndSend(Demo7255Application.MESSAGE_QUEUE, actionMap);
            	template.convertAndSend(Demo7255Application.topicExchange, "sanket.indexing.queue", actionMap);
				
			}


            //delete the cache
            this.cacheMap.remove(objectId);
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        }
    }

    public void sendToIndex(JSONObject json, String pid){
		try {
			JSONObject newJson = new JSONObject();

			for (Object key : json.keySet()) {
				String attributeKey = String.valueOf(key);
				Object attributeVal = json.get(String.valueOf(key));
				Object id = json.get("objectId");
				if (attributeKey.equals("planserviceCostShares")){
					JSONObject obj = (JSONObject) attributeVal;
					obj.put("objectType","planservice_membercostshare");
					attributeVal = obj;
				}
				if (attributeVal instanceof JSONObject) {
					sendToIndex((JSONObject) attributeVal, (String) id);
				} else if (attributeVal instanceof JSONArray) {
					JSONArray arr = json.getJSONArray((String) key);
					for (int i = 0; i < arr.length(); i++) {
						sendToIndex(arr.getJSONObject(i), (String) id);
					}
				} else {
					newJson.put(attributeKey, attributeVal);
				}
			}

			JSONObject emd = new JSONObject();
			boolean isChild = false;
			emd.put("name", (String) json.get("objectType"));
			if (pid.length() != 0) {
				emd.put("parent", pid);
				isChild = true;
			}
			newJson.put("plan_service", emd);
			String id = json.getString("objectId");
			System.out.println("NEW JSON:\n"+ newJson.toString());
//			kafkaService.publish(newJson.toString(), "patch");
//			indexJsonObj(uri, indexName, newJson, id, isChild);
			publishToRabbitMq(newJson.toString(), id, isChild);
		}catch (Exception e){
//			logger.error("Kafka OR ES is shut down");
//			logger.error("Deleting from Redis");
			e.printStackTrace();
		}
	}
    
    private void publishToRabbitMq(String json, String id, boolean isChild) {
    	// index objects
        Map<String, String> actionMap = new HashMap<>();
        actionMap.put("operation", "SAVE");
        actionMap.put("uri", "http://localhost:9200");
        actionMap.put("index", "planindex");
        actionMap.put("id", id);
        actionMap.put("isChild", String.valueOf(isChild));
        actionMap.put("body", json);
        template.convertAndSend(Demo7255Application.topicExchange, "sanket.indexing.queue", actionMap);
        System.out.println("Sending message: " + actionMap);
//        template.convertAndSend(Demo7255Application.MESSAGE_QUEUE, actionMap);
    }
}
