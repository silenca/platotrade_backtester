import time

import telebot
from test.macd import MACD

from test.bot import config
from test.investing_com import InvestingCom

bot = telebot.TeleBot(config.api_key)


@bot.message_handler(commands=['start'])
def run_bot(message):
    macd = MACD('btc_usd', 12, 26, 9, 15, "15")
    bot.send_message(message.chat.id, f'You will be get trade notification with MACD settings 12 26 9 {macd.time_period}m')

    while True:
        macd.handling_coefficient()
        if (macd.last_calculation['advisor'] != 'HOLD').bool():
            current_rate = InvestingCom().current_rate()
            bot.send_message(message.chat.id, f'{macd.last_calculation}/n '
                                              f'current bid - {current_rate["bid"]}/n'
                                              f'current ask - {current_rate["ask"]}')
            print(f'{macd.last_calculation}/n '
                                              f'current bid - {current_rate["bid"]}/n'
                                              f'current ask - {current_rate["ask"]}')
        else:
            pass
        time.sleep(60)
if __name__ == '__main__':
    bot.polling(none_stop=True)