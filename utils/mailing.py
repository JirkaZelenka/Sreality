import smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from config import Config

class EmailService: 
    
    def __init__(self) -> None: 
        self.cf = Config() 

    def send_email(self, subject, message_text):
        
        creds = self.cf.mailing_credentials
        smtp_server = creds["server"]
        smtp_port = creds["port"]
        sender_email = creds["sender_email"]
        sender_password = creds["password"]
        receiver_email = creds["receiver_email"]
        
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = receiver_email
        msg['Subject'] = subject
        
        # Add message body
        msg.attach(MIMEText(message_text, 'plain'))
        
        text = msg.as_string()
        
        context = ssl.create_default_context()      
        
        # Connect to SMTP server and send email: STMP + server.starttls(),, or SMTP_SSL?
        with smtplib.SMTP_SSL(smtp_server, smtp_port, context = context) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_email, text)
            
    def send_email_with_attachment(self, subject, message_text, file_name):
        
        creds = self.cf.mailing_credentials
        smtp_server = creds["server"]
        smtp_port = creds["port"]
        sender_email = creds["sender_email"]
        sender_password = creds["password"]
        receiver_email = creds["receiver_email"]
        
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = receiver_email
        msg['Subject'] = subject
        
        # Add message body
        msg.attach(MIMEText(message_text, 'plain'))
        
        with open(file_name, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        encoders.encode_base64(part)

        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {file_name}",
        )
        msg.attach(part)

        text = msg.as_string()
        
        context = ssl.create_default_context()      
        
        # Connect to SMTP server and send email: STMP + server.starttls(),, or SMTP_SSL?
        with smtplib.SMTP_SSL(smtp_server, smtp_port, context = context) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_email, text)

