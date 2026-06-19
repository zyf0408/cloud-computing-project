import urllib.request
import urllib.parse
import json
import ssl

# 服务器信息
server_ip = '119.3.216.113'

# 尝试通过HTTP访问服务器（如果服务器有Web服务）
print(f"检查服务器 {server_ip} 的状态...")
print("-" * 50)

# 1. 检查ping
import subprocess
try:
    result = subprocess.run(['ping', '-n', '2', server_ip], capture_output=True, text=True, timeout=10)
    print("Ping测试:")
    print(result.stdout)
except Exception as e:
    print(f"Ping失败: {e}")

print("-" * 50)

# 2. 尝试HTTP访问（80端口）
try:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    req = urllib.request.Request(f"http://{server_ip}", method='HEAD', headers={'User-Agent': 'Mozilla/5.0'})
    response = urllib.request.urlopen(req, timeout=5, context=ctx)
    print(f"HTTP 80端口: 状态码 {response.status}")
except Exception as e:
    print(f"HTTP 80端口: 无法访问 ({type(e).__name__})")

# 3. 尝试HTTPS访问（443端口）
try:
    req = urllib.request.Request(f"https://{server_ip}", method='HEAD', headers={'User-Agent': 'Mozilla/5.0'})
    response = urllib.request.urlopen(req, timeout=5, context=ctx)
    print(f"HTTPS 443端口: 状态码 {response.status}")
except Exception as e:
    print(f"HTTPS 443端口: 无法访问 ({type(e).__name__})")

print("-" * 50)
print("总结:")
print("如果ping通但SSH(22)和HTTP(80/443)都不通，说明:")
print("1. 服务器网络正常")
print("2. 但服务未启动或防火墙阻止了连接")
print("3. 需要登录服务器检查SSH服务状态")
