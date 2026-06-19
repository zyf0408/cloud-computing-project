import paramiko
import time
import sys

# 服务器信息
host = '119.3.216.113'
username = 'root'
password = 'CC123456789！'

print(f"开始尝试连接服务器 {host}...")
print(f"用户名: {username}")
print(f"密码: {password}")
print("-" * 50)

# 尝试连接
for attempt in range(1, 31):
    try:
        print(f"第 {attempt} 次尝试...", end=' ')
        sys.stdout.flush()
        
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=host,
            username=username,
            password=password,
            timeout=10,
            banner_timeout=10,
            auth_timeout=10
        )
        
        print("成功!")
        print("-" * 50)
        print("SSH连接成功!")
        
        # 执行命令
        stdin, stdout, stderr = client.exec_command('whoami && hostname && uptime')
        output = stdout.read().decode()
        error = stderr.read().decode()
        
        if output:
            print("服务器响应:")
            print(output)
        if error:
            print("错误:")
            print(error)
        
        client.close()
        
        # 保存成功信息到文件
        with open('ssh_success.txt', 'w', encoding='utf-8') as f:
            f.write(f"SSH连接成功!\n")
            f.write(f"服务器: {host}\n")
            f.write(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"输出:\n{output}\n")
        
        break
        
    except paramiko.AuthenticationException as e:
        print(f"认证失败: {e}")
        print("密码可能不正确，请检查密码")
        break
    except paramiko.SSHException as e:
        print(f"SSH错误: {e}")
    except TimeoutError as e:
        print(f"连接超时")
    except Exception as e:
        print(f"失败: {type(e).__name__}: {e}")
    
    if attempt < 30:
        time.sleep(5)
else:
    print("-" * 50)
    print("30次尝试后仍然无法连接")
    print("可能的原因:")
    print("1. SSH服务未启动")
    print("2. 防火墙阻止了22端口")
    print("3. 安全组规则未生效")
    print("4. 服务器正在初始化")
