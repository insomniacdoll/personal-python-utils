# -*- coding: UTF-8 -*-
import smtplib

class SendMail(object):

    """docstring for SendMail"""
    def __init__(self, username, password, \
        smtpHost="smtp.gmail.com", smtpPort=587):
        self.smtpHost = smtpHost
        self.smtpPort = smtpPort
        self.username = username
        self.password = password

    def setSubject(self, emailSubject):
        self.emailSubject = emailSubject

    def setTo(self, recipient):
        self.recipient = recipient

    def setMailBody(self, emailBody):
        self.emailBody = emailBody

    def _initHeaderSession(self):
        if(hasattr(self, "emailSubject") and hasattr(self, "recipient") \
            and hasattr(self, "emailBody")):
            self.headers = "\r\n".join(["from: " + self.username, \
                           "subject: " + self.emailSubject, \
                           "to: " + self.recipient, \
                           "mime-version: 1.0", \
                           "content-type: text/html"])
            self.session = smtplib.SMTP(self.smtpHost, self.smtpPort)
        else:
            print "[ERROR] email subject, recipient and body must be set."

    def send(self):
        try:
            self._initHeaderSession()
            self.session.ehlo()
            self.session.starttls()
            self.session.login(self.username, self.password)
            self.content = self.headers + "\r\n\r\n" + self.emailBody
            self.session.sendmail(self.username, self.recipient, self.content)
        except Exception, e:
            print "[ERROR] exception occurred during mail sending."
            print "[ERROR] exception details: {0}".format(e)

if __name__ == '__main__':
    s = SendMail(username="test", password="test")
    s.setTo("test@test.com")
    s.setSubject("Hello world from Py Mail")
    s.setMailBody("Hello~~~")
    s.send()
