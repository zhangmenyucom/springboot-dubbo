package com.ang.provider.controller;

import com.ang.provider.MessageUpdator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;


/**
 * @author taylor
 */
@RestController
@RequestMapping("/test")
public class IndexController {
    @Autowired
    private MessageUpdator updator;

    @RequestMapping(value = "/test", method = RequestMethod.GET)
    public String test() {
        updator.getAndUpdateZNodeData();
        return "success";
    }


}
