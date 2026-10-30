"""
Downloader — async download, checksum verification, and retry logic.
"""

import asyncio
import os

import aiofiles
import aiohttp
import tqdm
from hashlib import sha256

from binance_ingestor.utils.log_kit import logger
from binance_ingestor.config import (
    BASE_URL, retry_times, thunder,
    file_proxy, need_analyse_set, daily_updated_set,
    create_download_semaphore,
)


def async_download_file(all_list, error_info_list):
    pbar = tqdm.tqdm(total=len(all_list), ncols=50, mininterval=0.5)

    async def _runner():
        sem = create_download_semaphore()
        tasks = download_file(all_list, pbar, error_info_list, sem)
        await asyncio.gather(*tasks)

    try:
        asyncio.run(_runner())
    finally:
        pbar.close()


def download_file(params, pbar, error_info_list, sem):
    tasks = []
    for param in params:
        key = param['key']
        download_checksum_url = f'{BASE_URL}{key}'
        sum_file_name = os.path.basename(param['key'])

        if not os.path.exists(param['local_path']):
            os.makedirs(param['local_path'])
        local_path = param['local_path']
        last_modified = param['last_modified']
        local_sum_path = os.path.join(local_path, sum_file_name)
        local_zip_path = os.path.join(local_path, sum_file_name[0:-9])

        if os.path.exists(local_sum_path):
            import datetime
            modify_utc_timestamp = datetime.datetime.utcfromtimestamp(os.path.getmtime(local_sum_path)).timestamp()
            if modify_utc_timestamp < last_modified:
                os.remove(local_sum_path)

        if os.path.exists(local_sum_path) and os.path.exists(local_zip_path):
            if not thunder:
                with open(local_sum_path, encoding='utf-8') as in_sum_file:
                    correct_sum = in_sum_file.readline().split(' ')[0]
                sha256Obj = sha256()
                with open(local_zip_path, 'rb') as in_zip_file:
                    sha256Obj.update(in_zip_file.read())
                if correct_sum == sha256Obj.hexdigest().lower():
                    pbar.update(1)
                    continue
            else:
                pbar.update(1)
                continue

        if 'monthly' in local_path:
            need_analyse_set.add(local_path)
        if 'daily_klines' in local_path:
            daily_updated_set.add(local_path)

        tasks.append(
            download(param['local_path'], download_checksum_url, local_sum_path, local_zip_path, pbar, error_info_list, sem))
    return tasks


async def download(local_path, download_checksum_url, local_sum_path, local_zip_path, pbar, error_info_list, sem):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        async with sem:
            retry = 0
            while True:
                if retry > retry_times and 'daily_klines' in local_path:
                    logger.warning(
                        f'下载 daily zip 失败次数超过 retry_times，已放弃: {local_zip_path}'
                        '（网络不稳定或数据包异常）'
                    )
                    error_info_list.add(f'放弃下载 {local_zip_path}（重试 {retry_times} 次）')
                    break
                try:
                    sum_file = await session.get(download_checksum_url, proxy=file_proxy, timeout=20)
                    sum_file_buffer = await sum_file.read()
                    async with aiofiles.open(local_sum_path, 'wb') as out_sum_file:
                        await out_sum_file.write(sum_file_buffer)

                    zip_file = await session.get(download_checksum_url[0:-9], proxy=file_proxy, timeout=20)
                    zip_file_buffer = await zip_file.read()
                    async with aiofiles.open(local_zip_path, 'wb') as out_zip_file:
                        await out_zip_file.write(zip_file_buffer)

                    async with aiofiles.open(local_sum_path, encoding='utf-8') as in_sum_file:
                        str_sum = await in_sum_file.read()
                        correct_sum = str_sum.split(' ')[0]
                    sha256_obj = sha256()
                    async with aiofiles.open(local_zip_path, 'rb') as in_zip_file:
                        sha256_obj.update(await in_zip_file.read())
                    if correct_sum == sha256_obj.hexdigest().lower():
                        pbar.update(1)
                        break
                except aiohttp.ClientError as ae:
                    error_info_list.add(f'下载{local_zip_path}失败，错误原因{ae}，已重试下载，请确认')
                except Exception as e:
                    error_info_list.add(f'下载{local_zip_path}失败，错误类型{type(e)}，已重试下载，请确认')
                retry += 1
