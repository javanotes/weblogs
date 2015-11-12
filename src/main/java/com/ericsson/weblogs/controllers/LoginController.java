/* ============================================================================
*
* FILE: LoginController.java
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
package com.ericsson.weblogs.controllers;

import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import lombok.extern.slf4j.Slf4j;
@Slf4j
@Controller
public class LoginController {

  @RequestMapping(value = { "/", "/signin" })
  public String goToSignOrHome() {
    String view = "index";

    Object principal = SecurityContextHolder.getContext().getAuthentication()
        .getPrincipal();

    

    log.info("View - {} \t For user - {}", view, principal);
    return view;
  }
}
