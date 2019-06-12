import java.lang.reflect.Array;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

class Constants{
    public static final Map<String, String> PREDECESSORS_MAP = createPredecessorMap();
    public static final String AVD_5554 = "5554";
    public static final String AVD_5556 = "5556";
    public static final String AVD_5558 = "5558";
    public static final String AVD_5560 = "5560";
    public static final String AVD_5562 = "5562";
    public static final String[] SORTED_KEY_SPACE = {AVD_5562, AVD_5556, AVD_5554, AVD_5558, AVD_5560};
    private static Map<String, String> createPredecessorMap(){
        Map<String, String> map = new HashMap<String, String>();
        map.put(AVD_5554, AVD_5556);
        map.put(AVD_5556, AVD_5562);
        map.put(AVD_5558, AVD_5554);
        map.put(AVD_5560, AVD_5558);
        map.put(AVD_5562, AVD_5560);
        return map;
    }
}

public class Test {
    final static String EMAIL_USERNAME = "kiranprabhakar135@gmail.com";
    final static String EMAIL_PWD = "";

    public static void main(String args[]){
        try {
            sendEmail("kprabhaasdfsadfsdfkasfasdfas@asfdsabuffaloedu", "Test", "body");
        } catch (MessagingException e) {
            e.printStackTrace();
        }
    }
    public  static  void sendEmail(String emailTo, String subject, String emailText) throws MessagingException {
        Properties properties = System.getProperties();
        properties.put("mail.smtp.auth", "true");
        properties.put("mail.smtp.starttls.enable", "true");
        properties.put("mail.smtp.host", "smtp.gmail.com");
        properties.put("mail.smtp.port", "587");
        Session session = Session.getDefaultInstance(properties, new javax.mail.Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(EMAIL_USERNAME, EMAIL_PWD);
            }
        });

        // Create a default MimeMessage object.
        MimeMessage message = new MimeMessage(session);
        // Set From: header field of the header.
        message.setFrom(new InternetAddress(EMAIL_USERNAME));
        // Set To: header field of the header.
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(emailTo));
        // Set Subject: header field
        message.setSubject(subject);
        // Now set the actual message
        message.setText(emailText);
        // Send message
        Transport.send(message);

    }
    public static String genHash(String input) {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            return formatter.toString();
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
    }

}


