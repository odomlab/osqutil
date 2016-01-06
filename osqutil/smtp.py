'''SMTP mail functions used for user and admin notifications.'''

from email.mime.text import MIMEText
import smtplib
from .config import Config

CONFIG = Config()

def email_admins(subject, body):
  '''
  Send an email to the admin users as registered in our repository database.
  '''
  from ..models import User
  recips = set([ u.email for u in User.objects.filter(is_superuser=True) ])
  send_email(subject, body, recips)

def send_email(subject, body, recips, include_admins=False):
  '''
  Send an email to a list of recipients, optionally including the
  registered admin users.
  '''
  if include_admins:
    from ..models import User
    recips = set(recips)
    recips = recips.union([ u.email for u
                            in User.objects.filter(is_superuser=True) ])

  mail = MIMEText(body.encode('ascii', 'replace'))
  mail['Subject'] = subject.encode('ascii', 'replace')
  mail['From'] = CONFIG.smtp_sender
  mail['To'] = ",".join(recips)

  conn = smtplib.SMTP(host=CONFIG.smtp_server)
  conn.sendmail(CONFIG.smtp_sender, recips, str(mail))
  conn.close()
