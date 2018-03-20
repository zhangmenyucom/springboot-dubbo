package com.ang.provider.controller;

import com.ang.provider.MessageUpdator;
import com.ang.provider.TasksMaker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


/**
 * @author taylor
 */
@RestController
@RequestMapping("/test")
public class IndexController {
    @Autowired
    private MessageUpdator updator;

    @Autowired
    private TasksMaker tasksMaker;

    @RequestMapping(value = "/test", method = RequestMethod.GET)
    public String test() {
        updator.getAndUpdateZNodeData();
        return "success";
    }

    @RequestMapping(value = "/new_task", method = RequestMethod.GET)
    public String newTaks(@RequestParam("content") String content) {
        tasksMaker.createTask(content);
        return "success";
    }


}
