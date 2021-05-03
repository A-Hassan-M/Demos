/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tgs.unittestingdemo;

import static junit.framework.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author hassan
 */
public class TestJunit {
    
    /**
     * Test of main method, of class TestRunner.
     */
   @Test
    public void testSetup() {
        String str = "I am done with Junit setup";
        assertEquals("I am done with Junit setup", str);
    }
    
}
