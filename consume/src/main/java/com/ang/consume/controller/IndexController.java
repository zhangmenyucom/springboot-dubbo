package com.ang.consume.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.ang.provider.service.IndexService;


@Controller
public class IndexController {
	
	@Autowired
	private IndexService indexService;
	
	
	@ResponseBody
	@RequestMapping(value = "/index", method = RequestMethod.GET)
	public String findServer(){
		return indexService.getUserName();
	}
	

}
