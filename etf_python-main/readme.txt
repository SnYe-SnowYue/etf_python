安裝說明書
1. 請先安裝python3.8+環境，並安裝mysql資料庫
2. 請安裝如下指令:
(1) pip install flask
(2) pip install flask-session
(3) pip install mysql-connector-python
(4) pip install requests

3. 執行主程式
python app.py
如需要裝到iis請告知管理員，會教你如何裝。

4. 程式碼連結: https://github.com/willy1106101/etf_python
5. 資料庫: 
--- etf mysql資料庫
SET NAMES utf8;
SET time_zone = '+00:00';
SET foreign_key_checks = 0;
SET sql_mode = 'NO_AUTO_VALUE_ON_ZERO';

SET NAMES utf8mb4;

CREATE DATABASE `etf` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `etf`;

DROP TABLE IF EXISTS `users`;
CREATE TABLE `users` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(50) NOT NULL,
  `tel` int NOT NULL,
  `email` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `birthday` date NOT NULL,
  `idcard` varchar(20) NOT NULL,
  `username` varchar(255) NOT NULL,
  `password` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `username` (`username`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4;

其他資料庫內容暫時不放入
