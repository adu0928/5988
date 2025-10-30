import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import schedule
from binance.client import Client
from binance.exceptions import BinanceAPIException

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('C:\\Users\\Administrator\\Desktop\\alpha8888\\balance_log.txt'),
        logging.StreamHandler()
    ]
)

# 存储前一次净值
previous_net_values = {}


def load_config(config_path: str):
    """加载账户配置文件并返回账户列表与每日群通知配置"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        logging.error(f"加载账户配置文件失败: {e}")
        raise

    accounts: List[Dict[str, Any]]
    daily_report_webhook: Optional[str] = None

    if isinstance(data, dict):
        accounts = data.get('accounts', [])
        daily_report_webhook = data.get('daily_report_webhook_url') or data.get('daily_webhook_url')
    elif isinstance(data, list):
        accounts = data
    else:
        raise ValueError('账户配置文件格式不正确，应为数组或包含 accounts 字段的对象。')

    # 兼容账户级别配置 daily_webhook_url
    if not daily_report_webhook:
        for account in accounts:
            if account.get('daily_webhook_url'):
                daily_report_webhook = account['daily_webhook_url']
                break

    return accounts, daily_report_webhook


def calculate_utf8_bytes(text: str) -> int:
    """精确计算UTF-8字节长度"""
    return len(text.encode('utf-8'))


def smart_split_message(content: str, max_bytes_per_segment: int = 3500) -> list:
    """
    智能分段消息，按UTF-8字节长度分割，保持逻辑完整性

    Args:
        content: 要分割的内容
        max_bytes_per_segment: 每段最大字节数

    Returns:
        分段后的内容列表
    """
    if calculate_utf8_bytes(content) <= max_bytes_per_segment:
        return [content]

    segments = []
    lines = content.split('\n')
    current_segment = ""
    current_bytes = 0

    i = 0
    while i < len(lines):
        line = lines[i]
        line_bytes = calculate_utf8_bytes(line + '\n')

        # 如果单行就超过限制，强制分割
        if line_bytes > max_bytes_per_segment:
            if current_segment:
                segments.append(current_segment.rstrip('\n'))
                current_segment = ""
                current_bytes = 0

            # 对超长行进行字符级分割
            char_segment = ""
            for char in line:
                char_bytes = calculate_utf8_bytes(char)
                if calculate_utf8_bytes(char_segment + char) > max_bytes_per_segment:
                    if char_segment:
                        segments.append(char_segment)
                    char_segment = char
                else:
                    char_segment += char

            if char_segment:
                current_segment = char_segment + '\n'
                current_bytes = calculate_utf8_bytes(current_segment)

        # 正常处理
        elif current_bytes + line_bytes > max_bytes_per_segment:
            # 当前段已满，开始新段
            if current_segment:
                segments.append(current_segment.rstrip('\n'))
            current_segment = line + '\n'
            current_bytes = line_bytes
        else:
            # 添加到当前段
            current_segment += line + '\n'
            current_bytes += line_bytes

        i += 1

    # 添加最后一段
    if current_segment:
        segments.append(current_segment.rstrip('\n'))

    return segments


def send_segmented_wecom_notification(title: str, content: str, webhook_url: str):
    """
    发送分段企业微信通知

    Args:
        title: 消息标题
        content: 消息内容（可能很长）
        webhook_url: 企业微信webhook地址
    """
    try:
        # 计算总长度
        total_bytes = calculate_utf8_bytes(title + content)
        title_bytes = calculate_utf8_bytes(title)
        content_bytes = calculate_utf8_bytes(content)

        logging.info(f"消息长度检查: 标题 {title_bytes} 字节, 内容 {content_bytes} 字节, 总计 {total_bytes} 字节")

        # 单条消息限制（预留标题和分段标识空间）
        max_content_bytes = 3500

        if total_bytes <= 4000:
            # 直接发送
            payload = {
                "msgtype": "markdown",
                "markdown": {
                    "content": f"{title}\n{content}"
                }
            }

            response = requests.post(webhook_url, json=payload, timeout=10)

            if response.status_code == 200 and response.json().get('errcode') == 0:
                logging.info(f"企业微信通知发送成功至 {webhook_url} (消息长度: {total_bytes} 字节)")
            else:
                logging.error(f"企业微信通知发送失败至 {webhook_url}: {response.text}")
        else:
            # 分段发送
            segments = smart_split_message(content, max_content_bytes)
            total_segments = len(segments)

            logging.info(f"消息过长，分为 {total_segments} 段发送")

            for i, segment in enumerate(segments, 1):
                # 构造分段标题
                segment_title = title.replace("###", f"### ({i}/{total_segments})")

                # 为非首段添加续接标识
                if i > 1:
                    segment = f"📄 续接第{i}部分\n\n{segment}"

                # 为非末段添加继续标识
                if i < total_segments:
                    segment += f"\n\n⏬ 继续查看第{i + 1}部分..."

                payload = {
                    "msgtype": "markdown",
                    "markdown": {
                        "content": f"{segment_title}\n{segment}"
                    }
                }

                segment_bytes = calculate_utf8_bytes(f"{segment_title}\n{segment}")

                response = requests.post(webhook_url, json=payload, timeout=10)

                if response.status_code == 200 and response.json().get('errcode') == 0:
                    logging.info(
                        f"企业微信通知第{i}/{total_segments}段发送成功至 {webhook_url} (长度: {segment_bytes} 字节)")
                else:
                    logging.error(f"企业微信通知第{i}/{total_segments}段发送失败至 {webhook_url}: {response.text}")

                # 分段发送间隔，避免频率限制
                if i < total_segments:
                    time.sleep(1)

    except Exception as e:
        logging.error(f"发送企业微信通知异常至 {webhook_url}: {e}")


def get_account_balances(api_key: str, api_secret: str):
    """查询单个账户余额 - 修改版：同时获取现货和资金钱包余额"""
    try:
        client = Client(api_key, api_secret)

        # 获取现货账户余额
        account_info = client.get_account()
        spot_balances = account_info['balances']

        # 获取资金钱包余额
        try:
            funding_balances = client.funding_wallet()
            logging.info("资金钱包余额获取成功")
        except BinanceAPIException as e:
            logging.warning(f"资金钱包余额获取失败: {e}, 将使用空列表")
            funding_balances = []
        except Exception as e:
            logging.warning(f"资金钱包余额获取异常: {e}, 将使用空列表")
            funding_balances = []

        # 合并处理现货和资金钱包余额
        combined_balances = []

        # 处理现货余额
        for balance in spot_balances:
            if float(balance['free']) > 0 or float(balance['locked']) > 0:
                combined_balances.append({
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'account_id': None,  # 稍后填充
                    'asset': balance['asset'],
                    'spot_free': float(balance['free']),
                    'spot_locked': float(balance['locked']),
                    'funding_free': 0.0,  # 默认为0，稍后更新
                    'funding_locked': 0.0,
                    'funding_freeze': 0.0,
                    'funding_withdrawing': 0.0
                })

        # 处理资金钱包余额
        funding_dict = {}
        for funding in funding_balances:
            asset = funding['asset']
            funding_dict[asset] = {
                'free': float(funding['free']),
                'locked': float(funding['locked']),
                'freeze': float(funding.get('freeze', 0)),
                'withdrawing': float(funding.get('withdrawing', 0))
            }

        # 合并资金钱包数据到现有记录
        asset_found = set()
        for balance in combined_balances:
            asset = balance['asset']
            if asset in funding_dict:
                balance['funding_free'] = funding_dict[asset]['free']
                balance['funding_locked'] = funding_dict[asset]['locked']
                balance['funding_freeze'] = funding_dict[asset]['freeze']
                balance['funding_withdrawing'] = funding_dict[asset]['withdrawing']
                asset_found.add(asset)

        # 添加只在资金钱包中存在的资产
        for asset, funding_data in funding_dict.items():
            if asset not in asset_found and (funding_data['free'] > 0 or funding_data['locked'] > 0):
                combined_balances.append({
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'account_id': None,  # 稍后填充
                    'asset': asset,
                    'spot_free': 0.0,
                    'spot_locked': 0.0,
                    'funding_free': funding_data['free'],
                    'funding_locked': funding_data['locked'],
                    'funding_freeze': funding_data['freeze'],
                    'funding_withdrawing': funding_data['withdrawing']
                })

        return combined_balances

    except BinanceAPIException as e:
        logging.error(f"币安API错误: {e}")
        return None
    except Exception as e:
        logging.error(f"查询余额失败: {e}")
        return None


def batch_query_balances(
    config_path: str, accounts: Optional[List[Dict[str, Any]]] = None
):
    """批量查询多个账户余额"""
    if accounts is None:
        accounts, _ = load_config(config_path)
    all_balances = []

    for account in accounts:
        account_id = account['account_id']
        api_key = account['api_key']
        api_secret = account['api_secret']

        logging.info(f"正在查询账户: {account_id}")
        balances = get_account_balances(api_key, api_secret)

        if balances:
            for balance in balances:
                balance['account_id'] = account_id
                all_balances.append(balance)
        else:
            logging.warning(f"账户 {account_id} 查询失败")

    return all_balances


def calculate_total_balance(balances: list):
    """计算所有账户的总金额（按资产汇总） - 修改版：包含资金钱包"""
    total_by_asset = {}
    for balance in balances:
        asset = balance['asset']
        spot_total = balance['spot_free'] + balance['spot_locked']
        funding_total = balance['funding_free'] + balance['funding_locked']
        combined_total = spot_total + funding_total

        if asset not in total_by_asset:
            total_by_asset[asset] = {
                'spot_total': 0,
                'funding_total': 0,
                'combined_total': 0
            }

        # 根据资产调整精度
        if asset == 'BNB':
            total_by_asset[asset]['spot_total'] = round(total_by_asset[asset]['spot_total'] + spot_total, 6)
            total_by_asset[asset]['funding_total'] = round(total_by_asset[asset]['funding_total'] + funding_total, 6)
            total_by_asset[asset]['combined_total'] = round(total_by_asset[asset]['combined_total'] + combined_total, 6)
        else:
            total_by_asset[asset]['spot_total'] = round(total_by_asset[asset]['spot_total'] + spot_total, 2)
            total_by_asset[asset]['funding_total'] = round(total_by_asset[asset]['funding_total'] + funding_total, 2)
            total_by_asset[asset]['combined_total'] = round(total_by_asset[asset]['combined_total'] + combined_total, 2)

    return total_by_asset


def save_to_excel(balances: list, output_path: str):
    """保存余额到Excel（追加模式） - 修改版：添加资金钱包列"""
    try:
        df = pd.DataFrame(balances)
        if os.path.exists(output_path):
            existing_df = pd.read_excel(output_path)
            df = pd.concat([existing_df, df], ignore_index=True)
        df.to_excel(output_path, index=False)
        logging.info(f"结果已保存至: {output_path}")
    except Exception as e:
        logging.error(f"保存Excel失败: {e}")


def print_balances(balances: list):
    """格式化输出余额 - 修改版：显示现货和资金钱包"""
    if not balances:
        print("无可用余额或查询失败")
        return

    print("\n账户余额（现货+资金钱包）：")
    print("-" * 120)
    print(
        f"{'时间':<20} {'账户ID':<15} {'资产':<10} {'现货可用':<12} {'现货锁定':<12} {'资金可用':<12} {'资金锁定':<12} {'合计余额':<12}")
    print("-" * 120)

    for balance in balances:
        spot_total = balance['spot_free'] + balance['spot_locked']
        funding_total = balance['funding_free'] + balance['funding_locked']
        combined_total = spot_total + funding_total

        print(f"{balance['timestamp']:<20} {balance['account_id']:<15} {balance['asset']:<10} "
              f"{balance['spot_free']:<12.6f} {balance['spot_locked']:<12.6f} "
              f"{balance['funding_free']:<12.6f} {balance['funding_locked']:<12.6f} "
              f"{combined_total:<12.6f}")
    print("-" * 120)


def calculate_daily_change(output_path: str):
    """计算每日余额变化 - 修改版：包含资金钱包变化"""
    try:
        if not os.path.exists(output_path):
            logging.info("无历史数据，无法计算余额变化")
            return None, None, None

        df = pd.read_excel(output_path)
        if df.empty:
            return None, None, None

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)

        df_today = df[df['timestamp'].dt.date == today]
        df_yesterday = df[df['timestamp'].dt.date == yesterday]

        changes = []
        total_today = {}
        total_yesterday = {}

        for account_id in df['account_id'].unique():
            for asset in df[df['account_id'] == account_id]['asset'].unique():
                today_data = df_today[(df_today['account_id'] == account_id) & (df_today['asset'] == asset)]
                yesterday_data = df_yesterday[
                    (df_yesterday['account_id'] == account_id) & (df_yesterday['asset'] == asset)]

                # 计算今日余额
                today_spot_total = 0
                today_funding_total = 0
                today_snapshot_ts = None
                if not today_data.empty:
                    today_snapshot = today_data.sort_values('timestamp').iloc[-1]
                    today_spot_total = today_snapshot['spot_free'] + today_snapshot['spot_locked']
                    today_funding_total = today_snapshot['funding_free'] + today_snapshot['funding_locked']
                    today_snapshot_ts = today_snapshot['timestamp']

                # 计算昨日余额
                yesterday_spot_total = 0
                yesterday_funding_total = 0
                if not yesterday_data.empty:
                    yesterday_snapshot = yesterday_data.sort_values('timestamp').iloc[-1]
                    yesterday_spot_total = yesterday_snapshot['spot_free'] + yesterday_snapshot['spot_locked']
                    yesterday_funding_total = (
                        yesterday_snapshot['funding_free'] + yesterday_snapshot['funding_locked'])

                spot_change = today_spot_total - yesterday_spot_total
                funding_change = today_funding_total - yesterday_funding_total
                total_change = (today_spot_total + today_funding_total) - (
                            yesterday_spot_total + yesterday_funding_total)

                if total_change != 0 or (today_spot_total + today_funding_total) != 0:
                    changes.append({
                        'account_id': account_id,
                        'asset': asset,
                        'spot_total': round(today_spot_total, 2 if asset != 'BNB' else 6),
                        'funding_total': round(today_funding_total, 2 if asset != 'BNB' else 6),
                        'combined_total': round(today_spot_total + today_funding_total, 2 if asset != 'BNB' else 6),
                        'spot_change': round(spot_change, 2 if asset != 'BNB' else 6),
                        'funding_change': round(funding_change, 2 if asset != 'BNB' else 6),
                        'total_change': round(total_change, 2 if asset != 'BNB' else 6),
                        'timestamp': today_snapshot_ts if today_snapshot_ts is not None else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })

                # 更新汇总数据
                combined_today = today_spot_total + today_funding_total
                combined_yesterday = yesterday_spot_total + yesterday_funding_total
                total_today[asset] = total_today.get(asset, 0) + combined_today
                total_yesterday[asset] = total_yesterday.get(asset, 0) + combined_yesterday

        # 调整精度
        for asset in total_today:
            total_today[asset] = round(total_today[asset], 2 if asset != 'BNB' else 6)
            total_yesterday[asset] = round(total_yesterday[asset], 2 if asset != 'BNB' else 6)

        return changes, total_today, total_yesterday
    except Exception as e:
        logging.error(f"计算余额变化失败: {e}")
        return None, None, None


def check_risk_alert(current_balances, accounts):
    """检查净值减少超过100 USDT，发送风险提示 - 修改版：包含资金钱包"""
    global previous_net_values
    for account in accounts:
        account_id = account['account_id']
        current_account_balances = [b for b in current_balances if b['account_id'] == account_id]

        # 计算当前总净值（现货+资金钱包）
        current_net_value = sum(
            b['spot_free'] + b['spot_locked'] + b['funding_free'] + b['funding_locked']
            for b in current_account_balances
        )

        prev_net_value = previous_net_values.get(account_id, current_net_value)  # 首次使用当前值
        net_value_drop = prev_net_value - current_net_value

        if net_value_drop > 100:  # 净值减少超过100 USDT
            risk_webhook_url = account.get('risk_webhook_url')
            if risk_webhook_url:
                content = f"【账户ID: {account_id}】\n"

                # 简化风险提示内容，避免超长
                asset_summary = {}
                for balance in current_account_balances:
                    asset = balance['asset']
                    total = balance['spot_free'] + balance['spot_locked'] + balance['funding_free'] + balance[
                        'funding_locked']
                    asset_summary[asset] = asset_summary.get(asset, 0) + total

                for asset, total in asset_summary.items():
                    if total > 0:
                        total_str = f"{total:.6f}" if asset == 'BNB' else f"{total:.2f}"
                        content += f"  {asset}: {total_str}\n"

                content += f"  净值减少: {net_value_drop:.2f} USDT\n"
                content += f"  当前净值: {current_net_value:.2f} USDT"

                send_segmented_wecom_notification(
                    f"### ⚠️ 风险提示 ({current_balances[0]['timestamp']})",
                    content,
                    risk_webhook_url
                )
                logging.warning(
                    f"风险提示发送至 {risk_webhook_url}，账户 {account_id} 净值减少 {net_value_drop:.2f} USDT")

        previous_net_values[account_id] = current_net_value  # 更新净值


def job():
    """定时任务：查询余额并发送通知 - 分段消息版"""
    config_path = os.path.join(os.path.dirname(__file__), 'accounts.json')
    output_path = os.path.join(os.path.dirname(__file__), 'balances.xlsx')

    try:
        accounts, _ = load_config(config_path)
        balances = batch_query_balances(config_path, accounts)
        print_balances(balances)
        if balances:
            save_to_excel(balances, output_path)
            # 按webhook_url分组账户
            group_balances = {}
            for balance in balances:
                account_id = balance['account_id']
                # 查找账户的webhook_url
                webhook_url = next((a['webhook_url'] for a in accounts if a['account_id'] == account_id), None)
                if webhook_url:
                    if webhook_url not in group_balances:
                        group_balances[webhook_url] = []
                    group_balances[webhook_url].append(balance)

            # 为每个群生成并发送通知
            for webhook_url, group_balance in group_balances.items():
                # 生成完整详细内容（不压缩）
                content = ""
                for balance in group_balance:
                    content += f"【账户ID: {balance['account_id']}】\n"
                    content += f"  资产: {balance['asset']}\n"

                    # 现货余额显示
                    spot_free_str = f"{balance['spot_free']:.6f}" if balance[
                                                                         'asset'] == 'BNB' else f"{balance['spot_free']:.2f}"
                    spot_locked_str = f"{balance['spot_locked']:.6f}" if balance[
                                                                             'asset'] == 'BNB' else f"{balance['spot_locked']:.2f}"
                    content += f"  现货可用: {spot_free_str}\n"
                    content += f"  现货锁定: {spot_locked_str}\n"

                    # 资金钱包余额显示
                    funding_free_str = f"{balance['funding_free']:.6f}" if balance[
                                                                               'asset'] == 'BNB' else f"{balance['funding_free']:.2f}"
                    funding_locked_str = f"{balance['funding_locked']:.6f}" if balance[
                                                                                   'asset'] == 'BNB' else f"{balance['funding_locked']:.2f}"
                    content += f"  资金可用: {funding_free_str}\n"
                    content += f"  资金锁定: {funding_locked_str}\n"

                    # 合计余额
                    total = balance['spot_free'] + balance['spot_locked'] + balance['funding_free'] + balance[
                        'funding_locked']
                    total_str = f"{total:.6f}" if balance['asset'] == 'BNB' else f"{total:.2f}"
                    content += f"  合计余额: {total_str}\n\n"

                # 计算总金额汇总
                group_total_balance = {}
                for balance in group_balance:
                    asset = balance['asset']
                    spot_total = balance['spot_free'] + balance['spot_locked']
                    funding_total = balance['funding_free'] + balance['funding_locked']
                    combined_total = spot_total + funding_total

                    if asset not in group_total_balance:
                        group_total_balance[asset] = {'spot': 0, 'funding': 0, 'combined': 0}

                    if asset == 'BNB':
                        group_total_balance[asset]['spot'] = round(group_total_balance[asset]['spot'] + spot_total, 6)
                        group_total_balance[asset]['funding'] = round(
                            group_total_balance[asset]['funding'] + funding_total, 6)
                        group_total_balance[asset]['combined'] = round(
                            group_total_balance[asset]['combined'] + combined_total, 6)
                    else:
                        group_total_balance[asset]['spot'] = round(group_total_balance[asset]['spot'] + spot_total, 2)
                        group_total_balance[asset]['funding'] = round(
                            group_total_balance[asset]['funding'] + funding_total, 2)
                        group_total_balance[asset]['combined'] = round(
                            group_total_balance[asset]['combined'] + combined_total, 2)

                content += "【总金额汇总 (当前群账户)】\n"
                for asset, totals in group_total_balance.items():
                    spot_str = f"{totals['spot']:.6f}" if asset == 'BNB' else f"{totals['spot']:.2f}"
                    funding_str = f"{totals['funding']:.6f}" if asset == 'BNB' else f"{totals['funding']:.2f}"
                    combined_str = f"{totals['combined']:.6f}" if asset == 'BNB' else f"{totals['combined']:.2f}"
                    content += f"{asset}: 现货 {spot_str} | 资金 {funding_str} | 合计 {combined_str}\n"

                title = f"### 每小时余额更新 (现货+资金钱包) ({balances[0]['timestamp']})"

                # 使用分段发送（自动处理长度）
                send_segmented_wecom_notification(title, content, webhook_url)

            # 检查风险提示 (独立于群分组)
            check_risk_alert(balances, accounts)
    except Exception as e:
        logging.error(f"批量查询或通知失败: {e}")


def daily_change_job():
    """每日任务：计算余额变化并发送通知 - 分段消息版"""
    output_path = os.path.join(os.path.dirname(__file__), 'balances.xlsx')
    wecom_config_path = os.path.join(os.path.dirname(__file__), 'accounts.json')

    try:
        changes, total_today, total_yesterday = calculate_daily_change(output_path)
        if not changes and not total_today:
            logging.info("暂无每日余额变化记录，无需发送每日通知")
            return

        accounts, daily_report_webhook = load_config(wecom_config_path)
        if not daily_report_webhook:
            logging.warning("未配置每日统计的群通知地址，跳过每日通知发送")
            return

        # 账户维度的变动信息
        account_changes: Dict[str, List[Dict[str, Any]]] = {}
        if changes:
            for change in changes:
                account_changes.setdefault(change['account_id'], []).append(change)

        content_lines = []

        if account_changes:
            content_lines.append("【账户余额变化】")
            for account in accounts:
                acc_changes = account_changes.get(account['account_id'])
                if not acc_changes:
                    continue

                content_lines.append(f"账户 {account['account_id']}")
                for change in acc_changes:
                    spot_str = f"{change['spot_total']:.6f}" if change['asset'] == 'BNB' else f"{change['spot_total']:.2f}"
                    funding_str = f"{change['funding_total']:.6f}" if change['asset'] == 'BNB' else f"{change['funding_total']:.2f}"
                    combined_str = f"{change['combined_total']:.6f}" if change['asset'] == 'BNB' else f"{change['combined_total']:.2f}"
                    spot_change_str = f"{change['spot_change']:+.6f}" if change['asset'] == 'BNB' else f"{change['spot_change']:+.2f}"
                    funding_change_str = f"{change['funding_change']:+.6f}" if change['asset'] == 'BNB' else f"{change['funding_change']:+.2f}"
                    total_change_str = f"{change['total_change']:+.6f}" if change['asset'] == 'BNB' else f"{change['total_change']:+.2f}"

                    content_lines.append(
                        f"  · {change['asset']} | 现货 {spot_str} ({spot_change_str}) | 资金 {funding_str} ({funding_change_str}) | 合计 {combined_str} ({total_change_str})"
                    )
                content_lines.append("")

        if total_today:
            content_lines.append("【所有账户资产汇总】")
            for asset, today_total in total_today.items():
                yesterday_total = total_yesterday.get(asset, 0) if total_yesterday else 0
                change_total = today_total - yesterday_total
                if asset == 'BNB':
                    today_str = f"{today_total:.6f}"
                    yesterday_str = f"{yesterday_total:.6f}"
                    change_str = f"{change_total:+.6f}"
                else:
                    today_str = f"{today_total:.2f}"
                    yesterday_str = f"{yesterday_total:.2f}"
                    change_str = f"{change_total:+.2f}"

                content_lines.append(
                    f"{asset}: 今日 {today_str} | 昨日 {yesterday_str} | 变化 {change_str}"
                )

        content = '\n'.join(line for line in content_lines if line is not None)
        title = f"### 每日余额变化报告 (现货+资金钱包) ({datetime.now().strftime('%m-%d')})"

        send_segmented_wecom_notification(title, content, daily_report_webhook)
    except Exception as e:
        logging.error(f"每日余额变化任务失败: {e}")


def main():
    """主函数：设置定时任务"""
    # 每1小时查询并通知
    schedule.every(1).hours.do(job)
    # 每天00:00 JST 计算变化并通知
    schedule.every().day.at("00:00").do(daily_change_job)
    # 立即运行一次查询
    job()

    # 循环运行
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
