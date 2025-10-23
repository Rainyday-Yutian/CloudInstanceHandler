import hmac,hashlib,base64,requests,json,urllib.parse,datetime,netifaces,sys

def getHostIP(ifname):
    try:
        addrs = netifaces.ifaddresses(ifname)
        if netifaces.AF_INET in addrs:
            IP = addrs[netifaces.AF_INET][0]['addr']
        else:
            IP = "<IP_Address_Notfound>"
    except:
        IP = "<NIC_Interface_Notfound>"
    return IP

def send_ding_message(title,message,more_info=True,content_color=None,at_mobiles=None,is_at_all=False,webhook=None,secret=None,debug=False):
    # 当指定了secret和webhook参数时，以下默认配置不生效
    if (secret is None and webhook is None) or debug:
        # 不指定时默认发到如下
        webhook = ''
        secret = ''
    if sys.getsizeof(message) > 12000:
        message = message[:12000]+"...  \n\n<font color=red>send_ding_message:Message is too long. 超过接口限制触发强制截断</font>  \n"
    Now_Datetime = datetime.datetime.now()
    timestamp = str(round(Now_Datetime.timestamp() * 1000))
    if more_info:
        message += f"> {getHostIP('eth0')}:{__file__}  \n{Now_Datetime.strftime('%Y-%m-%d %H:%M:%S')}  \n"
    if at_mobiles and at_mobiles != '':
        at_mobiles = at_mobiles.split(',')
        message+="> At："
        for mobile in at_mobiles:
            message += f"@{mobile}"
    if content_color:
        message = f"<font color={content_color}>\n\n" + message + "</font>"
    if secret is not None and webhook is not None:
        secret_enc = secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        url = webhook+'&timestamp='+timestamp+'&sign='+sign
    else:
        url = webhook
    headers = {'Content-Type': 'application/json'}
    data = {
        'msgtype': 'markdown',  
        'markdown': {'title':title,'text': message}, 
        'at': {
            'atMobiles': at_mobiles if at_mobiles else [],
            'isAtAll': is_at_all 
        }
    }
    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        if response.json()['errcode'] == 0:
            print("Message sent to DingTalk successfully!")
        else:
            print("Failed to send message to DingTalk: ",response.json()['errmsg'])
            print('Webhook:',webhook)
    else:   
        print("Failed to send message to DingTalk, status code:",response.status_code)
        # 发送邮件等冗余操作

def send_ding_text(title,message,more_info=True,content_color=None,secret=None,webhook=None,debug=False):
    # 当指定了secret和webhook参数时，以下默认配置不生效
    if (secret is None and webhook is None) or debug:
        # 不指定时默认配置
        webhook = ''
        secret = ''
    Now_Datetime = datetime.datetime.now()
    timestamp = str(round(Now_Datetime.timestamp() * 1000))

    if secret is not None and webhook is not None:
        secret_enc = secret.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, secret)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        url = webhook+'&timestamp='+timestamp+'&sign='+sign
    else:
        url = webhook
    headers = {'Content-Type': 'application/json'}
    data = {'msgtype': 'text',  'markdown': {'title':title,'text': message}}
    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        if response.json()['errcode'] == 0:
            print("Message sent to DingTalk successfully!")
        else:
            print("Failed to send message to DingTalk: ",response.json()['errmsg'])
            print('Webhook:',webhook)
    else:   
        print("Failed to send message to DingTalk, status code:",response.status_code)
        # 发送邮件等冗余操作

