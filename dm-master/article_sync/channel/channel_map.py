# -*- coding: utf-8 -*-

from channel.youhui import youhui
from channel.yuanchuang import yuanchuang
from channel.zixun import zixun
from channel.pingce import pingce
from channel.wiki import wiki
from channel.xinruipinpai import xinruipinpai
from channel.shipin import shipin
from channel.machine_report import machine_report
from channel.home import home
from channel.online2offline import online2offline


CHANNEL_MAP = {
	"youhui": youhui,
	"yuanchuang": yuanchuang,
	"zixun": zixun,
	"pingce": pingce,
	"wiki": wiki,
	"xinruipinpai": xinruipinpai,
	"shipin": shipin,
	"machine_report": machine_report,
	"home": home,
	"online2offline": online2offline
}
