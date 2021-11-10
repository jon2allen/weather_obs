import subprocess
from email.message import EmailMessage


def send_email(from_addr, to_addrs, msg_subject, msg_body):
    msg = EmailMessage()
    msg.set_content(msg_body)
    print("from_addr: ", len(from_addr))
    print("to_addr: ", len(to_addrs))
    msg['From'] = from_addr
    msg['To'] = to_addrs
    msg['Subject'] = msg_subject

    print("msg : ", str(msg.as_bytes()))
    sendmail_location = "/usr/sbin/sendmail"
    subprocess.run([sendmail_location, "-t", "-oi"], input=msg.as_bytes())


def send_error_email(application):
    # email.cfg is a two line text file
    with open("email.cfg") as f:
        from_email_addr = r'{}'.format(f.readline())
        to_email_addr = r'{}'.format(f.readline())

    from_email_addr = from_email_addr.rstrip('\n')
    to_email_addr = to_email_addr.rstrip('\n')
    print("sending email")
    print("from: ", from_email_addr)
    print("to: ", to_email_addr)
    msg = "error has occurred in appliation: " + application
    subject = msg

    send_email(from_email_addr, to_email_addr, subject, msg)

    print("finished...")
