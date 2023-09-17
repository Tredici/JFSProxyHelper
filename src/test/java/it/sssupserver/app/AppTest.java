package it.sssupserver.app;

import static org.junit.Assert.assertTrue;

import java.net.UnknownHostException;

import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        assertTrue( true );
    }

    @Test
    public void testHostName() throws UnknownHostException {
        var hostname = App.getHostnameOrThrow();
        System.out.println("Hostname = " + hostname);
        assertTrue(hostname.equals("picci"));
    }
}
