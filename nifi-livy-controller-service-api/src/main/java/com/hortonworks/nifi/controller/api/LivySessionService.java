package com.hortonworks.nifi.controller.api;

import java.util.Map;

import org.apache.nifi.controller.ControllerService;

public interface LivySessionService extends ControllerService{
    Map<String, String> getSession();
}