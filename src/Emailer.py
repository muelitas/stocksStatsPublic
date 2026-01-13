from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

class Emailer:
  def __init__(self, user, pwd, host='smtp.gmail.com', port=465) -> None:
    self.__user = user
    self.__pwd = pwd
    self.__host = host # Default to Gmail SMTP server
    self.__port = port # Default to SSL port
    
    self._to = None
    self._from = None
    self._subject = None
    self._body = None

    # For now, we will only allow one attachment
    self._attachment_file_path = None
    self._attachment_file_name = None

    self._email = MIMEMultipart()

  #region Private Methods
  def __validate_email_params(self):
    # Ensure .to, .subject, and .body are set
    if not self._to or not self._subject or not self._body:
      raise ValueError("Email 'to', 'subject', and 'body' must be set.")
    
  def __validate_attachment_params(self):
    if (self._attachment_file_path and not self._attachment_file_name) or (not self._attachment_file_path and self._attachment_file_name):
      raise ValueError("Both 'attachment_file_path' and 'attachment_file_name' must be set together.")

  #endregion

  #region Public Methods
  def set_email_params(self, to, subject, body, _from=None):
    self._to = to
    self._subject = subject
    self._body = body
    self._from = self.__user if _from is None else _from

    self.__validate_email_params()

    self._email['Subject'] = self._subject
    self._email.attach(MIMEText(self._body))

  def set_attachment(self, attachment_file_path, attachment_file_name):
    self._attachment_file_path = attachment_file_path
    self._attachment_file_name = attachment_file_name
    self.__validate_attachment_params()

    with open(self._attachment_file_path, "rb") as attachment:
      self._email.attach(MIMEApplication(attachment.read(), Name=self._attachment_file_name))

  def send(self):
    try:
      server = smtplib.SMTP_SSL(self.__host, self.__port)
      server.ehlo()
      server.login(self.__user, self.__pwd)
      # TODO add validation to the expected delimiter
      server.sendmail(self._from, self._to.split(','), self._email.as_string())
      server.close()
      # TODO catch those recipients that failed
      print('Email sent!\n')
    except Exception as E:
      raise Exception(f"Error when sending email: {repr(E)}")
  #endregion