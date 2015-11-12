/* ============================================================================
*
* FILE: ContainsClauseParser.java
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
package com.ericsson.weblogs.dao;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

class ContainsClauseParser {

  static final String AND = "&&";
  static final String OR = "||";
  static final String OPEN = "(";
  static final String CLOSE = ")";
  static final String WS = " ";
  
  public static String parseAsClause(String expression, String term) 
  {
    //normalize
    expression = expression
    .replaceAll("\\&"+"\\&", WS + AND + WS)
    .replaceAll("\\"+"\\", WS + OR + WS)
    .replaceAll("\\"+OPEN, WS + OPEN + WS)
    .replaceAll("\\"+CLOSE, WS + CLOSE + WS);
    
    
    StringTokenizer st = new StringTokenizer(expression);
    String token;
    final StringBuilder out = new StringBuilder("(");
    
    while(st.hasMoreTokens())
    {
      token = st.nextToken();
      switch (token) {
      case AND:
        out.append(" and ");
        break;
      case OR:
        out.append(" or ");
        break;
      case OPEN:
        out.append(token);
        break;
      case CLOSE:
        out.append(token);
        break;

      default:
        out.append(term).append(" contains '").append(token).append("'");
        break;
      }
    }
    out.append(")");
    return out.toString();
  }
  
  public static List<String> parseToPostfix(final String _expression) throws ParseException
  {
    //normalize
    String expression = _expression
    .replaceAll("\\&"+"\\&", WS + AND + WS)
    .replaceAll("\\"+"\\", WS + OR + WS)
    .replaceAll("\\"+OPEN, WS + OPEN + WS)
    .replaceAll("\\"+CLOSE, WS + CLOSE + WS);
    
    if(!expression.startsWith(OPEN))
      expression = OPEN + expression + CLOSE;
    
    
    StringTokenizer st = new StringTokenizer(expression);
    String token;
    LinkedList<String> optStack = new LinkedList<>();
    List<String> out = new ArrayList<>();
    
    while(st.hasMoreTokens())
    {
      token = st.nextToken();
      switch (token) {
      case AND:
        if(!optStack.isEmpty() && !token.equals(optStack.peek()) && !OPEN.equals(optStack.peek()))
        {
          out.add(optStack.pop());
        }
        optStack.push(token);
        break;
      case OR:
        if(!optStack.isEmpty() && !token.equals(optStack.peek()) && !OPEN.equals(optStack.peek()))
        {
          out.add(optStack.pop());
        }
        optStack.push(token);
        break;
      case OPEN:
        optStack.push(token);
        break;
      case CLOSE:
        while(!optStack.isEmpty())
        {
          String s = optStack.pop();
          if(!OPEN.equals(s))
            out.add(s);
        }
        break;

      default:
        out.add(token);
        break;
      }
    }
    
    if(!optStack.isEmpty())
    {
      throw new ParseException("Inavalid expression!", -1);
    }
    return out;
  }
  
  public static void evaluateAsClause(List<String> postfix)
  {
    LinkedList<String> stack = new LinkedList<>();
    for(String term : postfix)
    {
      if(AND.equals(term))
      {
        stack.pop();
        stack.pop();
      }
      else if(OR.equals(term))
      {
        
      }
      else
      {
        stack.push(term);
      }
    }
  }
  
}
