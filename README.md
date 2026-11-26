# Template for the HighLoad course
This project is based on [Tiny Event Sourcing library](https://github.com/andrsuh/tiny-event-sourcing)

### Run PostgreSql
This example uses Postgres as an implementation of the Event store. You can see it in `pom.xml`:

```
<dependency>
    <groupId>ru.quipy</groupId>
    <artifactId>tiny-postgres-event-store-spring-boot-starter</artifactId>
    <version>${tiny.es.version}</version>
</dependency>
```

Thus, you have to run Postgres in order to test this example. Postgres service is included in  `docker-compose` file that we have in the root of the project.

# More comprehensive information about the course, project, how to run tests is here:

https://andrsuh.notion.site/2595d535059281d8a815c2cb3875c376?source=copy_link

https://andrsuh.notion.site/2625d5350592801aaf88c7c95302d10c?source=copy_link

### Run the infrastructure
Set of the services you need to start developing and testing process is following:
- Bombardier - service that is in charge of emulation the store's clients activity (creates the incoming load). Also serves as a third-party payment system.
- Postgres DBMS
- Prometheus + Grafana - metrics collection and visualization services

You can run all beforementioned services by the following command:
```
docker compose -f docker-compose.yml up
```

### Run the application
To make the application run you can start the main class `OnlineShopApplication`. It is not being launched as a docker contained to simplify and speed up the devevopment process as it is easier for you to refactor the application and re-run it immediately in the IDE.


### If you want to pull changes from the main repository into your fork

The command ```git remote -v``` should include the following lines:

```
upstream        https://github.com/andrsuh/high-load-course.git (fetch)
upstream        https://github.com/andrsuh/high-load-course.git (push)
```

If not, add the upstream remote:
```git remote add upstream https://github.com/andrsuh/high-load-course.git```

To pull changes from the main repository, run the following commands:

```
git fetch upstream
# switch to the main branch of your fork. Make sure the branch has no uncommitted changes to avoid conflicts
git checkout main 
# merge changes from the main repository into your main branch
git merge upstream/main 
```

## Лабораторная 7. Медленные ответы платёжного провайдера

Кейс использует `acc-7`, входную нагрузку 5 заказов в секунду, 1000 заказов и дедлайн платежа 6000 мс. Аккаунт допускает 8 вызовов в секунду и до 50 одновременных запросов. Каждый вызов стоит 30.

В калибровочном прогоне без короткого тайм-аута Bombardier зарегистрировал 108 успехов и 12 неудач на 120 заказов. Журнал провайдера показал два диапазона времени обработки: 108 вызовов заняли меньше 1 секунды, остальные 12 — от 9 до 9,5 секунды. При дедлайне 6 секунд ожидание медленного вызова расходует весь бюджет платежа. [Графики калибровки](evidence/hw-07/local-calibration.png).

Магазин ограничивает промежуточный вызов 1250 мс и может сделать до четырёх попыток с разными идентификаторами транзакций для одного платежа. Отправки распределены равномерно по лимиту аккаунта. После тайм-аута следующая попытка допускается, если до дедлайна хватает времени на ожидаемый успешный ответ и резерв 400 мс. Последней попытке доступен весь оставшийся бюджет за вычетом резерва.

Дашборд **HW-07 · acc-7** показывает число отправленных повторных вызовов по причине (`payment_retry_total`) и число платежей, по которым повтор вернул успех (`payment_recovered_total`). Если один платёж потребовал двух повторов и второй удался, счётчики вырастут на два и на один соответственно. На соседних панелях видны длительность вызовов, исход каждой попытки и независимые итоги клиента из Bombardier. [Конфигурация Grafana](grafana/provisioning/dashboards/HW07.json).

Для воспроизведения запусти `docker compose up -d`, задай `PAYMENT_SERVICE_NAME` и `PAYMENT_TOKEN` из своей конфигурации курса и выполни `mvn spring-boot:run`. Отправь первый запрос из [локального сценария](test-local-run.http) и выбери интервал прогона в [Grafana](http://localhost:3000/d/hw07-acc7/hw-07). Команда `mvn test` проверяет обработку временных и постоянных ошибок, медленного ответа, исчерпания числа попыток и истёкшего дедлайна.

### Локальный результат

25 ноября 2026 года Bombardier завершил 1000 заказов примерно за 3 минуты 20 секунд. Итоги рассчитаны по разности счётчиков до и после этого прогона.

| Показатель | Результат |
|---|---:|
| Успех / ошибка по данным Bombardier | 999 / 1 |
| Доход / расходы | 405 705 / 33 870 |
| Вызовы провайдера, включая повторы | 1 129 |
| Повторные вызовы по тайм-ауту | 129 |
| Успешные платежи после повтора | 111 |
| Отказы провайдера по лимиту запросов | 0 |

Из 999 успешных платежей 888 прошли с первой попытки, 96 — со второй и 15 — с третьей. Один платёж исчерпал четыре попытки и завершился клиентским тайм-аутом. Расходы совпали со стоимостью всех вызовов: 1 129 × 30 = 33 870. [Скриншот итогового прогона](evidence/hw-07/local-final.png).

### Удалённый результат

26 ноября 2026 года учебный стенд собрал `hw-07` на коммите `2733b75`. Первый платёж поступил в 10:49:27 МСК. К 10:52:52 Bombardier зарегистрировал завершение всех 1000 заказов. Интервал включает погрешность опроса счётчиков около 5–6 секунд.

| Показатель | Результат |
|---|---:|
| Успех / ошибка по данным Bombardier | 1 000 / 0 |
| Доход / расходы | 408 636 / 33 600 |
| Вызовы провайдера, включая повторы | 1 120 |
| Повторные вызовы | 120 |
| Успешные платежи после повтора | 109 |

У 109 разных платежей первая попытка завершилась тайм-аутом. Из них 98 прошли со второй попытки, 11 — с третьей. Остальные 891 прошли с первой. Суммарно журнал магазина содержит 120 тайм-аутов, что совпадает с числом дополнительных вызовов у провайдера. Стоимость 1 120 вызовов по 30 составила 33 600.

Графики построены по разностям счётчиков Bombardier и событиям журнала удалённого контейнера. Внизу изображения приведены скриншоты исходных строк удалённого экспортера. [Графики и скриншоты](evidence/hw-07/remote-final.png) · [Замеры по времени](evidence/hw-07/remote-samples.csv).
