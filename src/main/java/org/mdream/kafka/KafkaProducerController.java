package org.mdream.kafka;

import org.mdream.kafka.producer.ProducerService;
import org.mdream.kafka.producer.StringValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@Controller
public class KafkaProducerController {

    @Autowired
    private ProducerService producerService;

    /**
     * example http://localhost:8080/createStringValue?id=6&value=asdf
     * */
    @GetMapping(value = "/createStringValue")
    @ResponseBody
    public StringValue sendMessageToKafkaTopic(@RequestParam("id") Integer id, @RequestParam("value") String value) {
        StringValue stringValue = new StringValue(id, value);
        producerService.send(stringValue);
        return stringValue;
    }
}
