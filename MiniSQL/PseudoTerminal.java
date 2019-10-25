import java.io.IOException;
import java.io.*;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.*;

public class PseudoTerminal{

    public static void main(String[] args) {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String input = "";
        
        try {
            while (!input.equalsIgnoreCase("stop")) {
                showMenu();
                input = in.readLine();
                
                // Parsing the input string

                String[] parsedArgs = input.split(" ");
                
                switch(parsedArgs[0])
                {
                    case "load":    
                        System.out.println("Loading");
                        break;
                    
                    case "select":
                        
                        break;
                    
                    case "delete":
                        System.out.println("Delete");
                        break;
                    
                    case "exit":
                        System.exit(0);
                        break;

                }
                
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public static void showMenu() {
        System.out.println("MiniSQL::>");
    }

}