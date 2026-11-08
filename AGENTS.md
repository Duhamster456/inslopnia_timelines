# AGENTS.md — как строить таймлайны сессий

Инструкция для LLM-агента с доступом к shell и Python. Прочитай её целиком, прежде чем что-то записывать.
Если ты дорабатываешь код репозитория, а не строишь таймлайн, тебе нужны `README.md` и `docs/REFERENCE.md`.

## 1. Задача и результат

**Таймлайн** — журнал многоагентной сессии в формате JSONL, одна запись на строку. В нём видно:

- кто из агентов что делал и когда;
- как планировщик менял DAG шагов и кому поручал узлы;
- сколько ушло токенов и денег;
- какие были гипотезы, инциденты, кандидаты флага и чем всё закончилось.

`timeline.py` рендерит журнал в текст, HTML и SQLite. Человек смотрит результат в GUI (`gui.py`).

Режимы работы:

- **A. Разбор готового прогона inslopnia_harness.** Основной режим. На входе папка прогона `run/` (`ledger.sqlite3`, `pi-sessions/`). Скелет строит импорт, а ты дописываешь смысл, которого в ledger нет: решения, находки, гипотезы, инциденты, фазы. В конце пишешь короткий разбор.
- **B. Журнал по ходу работы.** Ты или оркестратор записываете события сразу, как они происходят. Так же ведут журнал по любым другим логам, для которых импорта нет: всё пишется командами CLI.

Что должно появиться в рабочей папке (это текущая директория, если в задании не сказано иное):

| файл | что это |
|---|---|
| `timeline.jsonl` | журнал, главный артефакт |
| `timeline.txt` | текстовый рендер |
| `timeline.html` | HTML-рендер, один файл, работает офлайн |
| `timeline.db` | SQLite для GUI и сравнения прогонов |
| `ANALYSIS.md` | разбор прогона, только в режиме A |

## 2. Инструмент

```bash
T="python /абсолютный/путь/к/репозиторию/timeline.py"   # репозиторий = папка, где лежит этот файл
$T -h                    # список команд
$T <команда> -h          # флаги команды; справка — источник истины, если она расходится с этим файлом
```

- **Файл журнала.** Указывай `-f timeline.jsonl` перед командой (`$T -f timeline.jsonl event …`) или задай `export TIMELINE_FILE=timeline.jsonl`. По умолчанию берётся `timeline.jsonl` в текущей папке.
- **Окружение.** Нужен только Python 3.10+ без пакетов. Вывод всегда в UTF-8, в том числе через pipe.
- **id записей.** Каждая команда записи печатает id созданной записи: `e12` (событие), `H1` (гипотеза), `I3` (инцидент). По нему потом ссылаются: `hyp set H1`, `incident resolve I3`, `--refs e12`. Номера не угадывай, бери тот, что напечатала команда. Новый id всегда на единицу больше самого большого занятого, поэтому с уже существующими он не совпадает. Если явно задать занятый `--hid` или `--iid`, команда откажется.
- **Списки** передаются через запятую: `--deps recon,research`.

**Время `--t`** (синоним `--at`):

| форма | пример |
|---|---|
| от начала сессии | `+12m`, `+1h5m30s`, `+90s`, `T+12:05`, `T+1:02:03` |
| абсолютное | epoch, ISO-8601 (`2026-10-31T15:56:57`), `HH:MM[:SS]` (в день старта сессии; если это больше чем на 12 ч раньше старта — на следующий день, то есть после полуночи) |
| сейчас | `now`, значение по умолчанию |

В режиме A время всегда бери из источника и передавай в виде `--t T+mm:ss`, как его печатает `transcript`. Значение по умолчанию (`now`) при разборе прошлого даст неверную картину. `validate` предупреждает о записях раньше начала и позже конца сессии.

## 3. Модель журнала

Журнал только дописывается. Состояние восстанавливается сворачиванием всех записей, поэтому порядок строк не важен, важно поле `t`.

| запись | команда | что значит | как видно в HTML |
|---|---|---|---|
| `session` | `init` | заголовок, задача (`--task`), цель, `--meta key=value` | шапка |
| `agent` | `agent` | агент: роль, модель, родитель; `--start`/`--stop` — интервал активности | дорожка агента |
| `event` | `event`, `note` | событие агента: `-k kind`, `-s step`, `--to` адресат, `--tokens` | маркер; при `--to` — стрелка |
| `dag` | `dag add/update/status/remove/dep-add/dep-rm` | правка плана; `--by` — автор правки, `--assignee` — исполнитель узла | ромб на дорожке автора, кривая к исполнителю, граф DAG |
| `usage` | `tokens` | расход токенов: дельта или `--cumulative` (нарастающий итог агента) | график токенов, KPI |
| `hyp` | `hyp add/set` | гипотеза: open → testing → confirmed / refuted / abandoned, с `-e` доказательством | список гипотез, маркер |
| `incident` | `incident add/update/mitigate/resolve` | баг или сбой: важность, категория, влияние, обход | карточка, маркер ⚠ |
| `flag` | `flag` | кандидат флага: candidate, submitted, accepted, rejected | маркер ⚑ |
| `outcome` | `outcome` | вердикт: solved, unsolved, partial, in_progress, abandoned | штамп в шапке, ★ |
| `phase` | `phase begin/end` | именованная фаза (recon, exploit…) | полоса над дорожками |
| — | `end` | время окончания сессии | правый край оси |

**Роли агентов:** `P` — планировщик, `E` — исполнитель, `R` — исследователь, `C` — критик, `operator` — человек или внешний агент, `runtime` — сам харнесс. Необъявленный агент создаётся автоматически, роль угадывается по id: `P`, `E:<шаг>`, `R:<шаг>`, `operator`, `runtime`.

**Виды событий (`-k`):** `action`, `observation`, `decision`, `hypothesis`, `milestone`, `message`, `result`, `tool`, `error`, `note`.

| вид | когда |
|---|---|
| `decision` | решения P: что делать, как разбить, что отменить |
| `milestone` | существенная находка или достигнутый промежуточный результат |
| `observation` | заметный факт без вывода |
| `result` | итог шага |
| `error` | ошибка агента или инструмента |
| `action` | обычное действие; не засоряй ими журнал |

## 4. Режим A: разбор прогона inslopnia_harness

### Что лежит в прогоне

```
run/
  ledger.sqlite3            состояние и журнал харнесса (таблицы events, steps, kv)
  pi-sessions/*.jsonl       полные переписки агентов:
                              *_planner-run.jsonl   — планировщик P
                              *_e-<шаг>-r<N>.jsonl  — исполнитель шага (N — номер попытки)
                              *_r-<шаг>-r<N>.jsonl  — исследователь
  logs/                     служебные логи (supervisor, runner, *.stderr)
  FINAL_REPORT.md, final_report.json   итог прогона, если он успел записаться
  config.snapshot.json      конфиг: модель, бюджеты, параллельность
  STOP                      есть — прогон остановил оператор
challenge.json              условие задачи (обычно рядом с run/ или уровнем выше)
```

Как устроен харнесс. Планировщик P строит DAG шагов и раздаёт их исполнителям E, иногда исследователю R. Исполнители сообщают находки планировщику через `report_milestone` и получают указания через inbox. Runtime следит за бюджетами: watchdog убивает шаг, превысивший `wall_timeout`. Оператор может вмешаться: добавить шаг, отменить шаг, остановить прогон.

### Что даёт импорт

`import-harness` читает `ledger.sqlite3` и `pi-sessions` и пишет в журнал:

- сессию с заголовком и целью из `challenge`, моделью из `config.snapshot.json`; `task` — имя папки над `run/`;
- агентов `P`, `E:<шаг>`, `R:<шаг>`, `operator`, `runtime` с интервалами активности;
- все правки и статусы DAG (`step_added`, `step_started`, `step_done`, `step_failed`, `step_cancelled`…) с автором правки;
- сообщения со стрелками: `step_report`, `executor_answer`, `exec_directive`, `operator_*`;
- токены и стоимость по агентам из `usage` в `pi-sessions` (бакеты `--bucket`, по умолчанию 60 с);
- автоинциденты: `step_failed`, `stale_*_killed`, аварийный конец хода планировщика. Их id вида `I-<шаг>-<попытка>`, `I-planner-<ход>`; категория по умолчанию `harness`, её надо проверить (см. раздел 7);
- вердикт и флаг из `run_finished` (флаг из строки `FLAG:`). Нет `run_finished` — вердикта не будет, его ставишь ты.

Импортированные записи имеют id `h<N>`. `import-harness --merge` переимпортирует прогон и сохраняет твои ручные записи.

### Порядок работы

0. **Разберись.** Выполни `$T -h` и `$T transcript -h`, посмотри `ls run/pi-sessions`, прочитай `challenge.json`. Пойми, какие агенты и шаги были, по именам файлов.
1. **Скелет.** `$T -f timeline.jsonl import-harness run`. Посмотри, что получилось: `$T -f timeline.jsonl render text --sections summary,agents,dag,incidents --detail 0`. Дальше только дописывай, импорт не повторяй (или повторяй с `--merge`).
2. **Читай переписки только через `transcript`.** Он печатает время в T+ этого журнала и сжимает переписку примерно в 10 раз:
   ```bash
   $T -f timeline.jsonl transcript run/pi-sessions/*_planner-run.jsonl --last 60
   $T -f timeline.jsonl transcript run/pi-sessions/*_e-recon-r0.jsonl -g 'flag|error|report_milestone|TASK_'
   $T -f timeline.jsonl transcript run/pi-sessions/*_e-recon-r0.jsonl --since T+10:00 --until T+15:00 --results 400
   $T -f timeline.jsonl transcript run/pi-sessions/*.jsonl -g 'hypothes|гипотез' --width 300   # несколько файлов = общая лента
   ```
   Сырые `.jsonl` целиком не выводи: это сжигает контекст. `--thinking` добавляет рассуждения модели (много текста, включай точечно). Сначала пройди P, чтобы понять план, потом исполнителей.
3. **Допиши смысл.** Время бери из `transcript`, все записи делай через CLI:
   - `phase begin/end` — фазы работы (разведка, эксплуатация, отладка, финал…), 2–6 на прогон;
   - `event … -k decision -s <шаг>` — ключевые решения P; при поручении добавь `--to E:<шаг>`;
   - `event E:<шаг> … -k milestone|result|observation|error -s <шаг> --to P` — находки исполнителей, ушедшие планировщику;
   - `hyp add` в момент появления гипотезы, `hyp set <H> confirmed|refuted|abandoned -e "<на чём основано>"` в момент проверки;
   - `incident add` для каждого реального сбоя или потери времени, затем `incident mitigate` или `incident resolve` по смыслу (раздел 7). Проверь каждый автоинцидент импорта;
   - `flag` — кандидаты флага. Статус `accepted` ставь, только если прогон или платформа подтвердили флаг;
   - `outcome`, если импорт не поставил вердикт: `solved` — флаг принят; `unsolved` — время вышло без флага; `abandoned` — прогон остановлен (`run/STOP`); `in_progress` — запись оборвалась, итог неизвестен.

   Ничего не выдумывай. Каждая запись должна опираться на конкретное место в файлах; при необходимости укажи его в `-d`.
4. **Проверь.** `$T -f timeline.jsonl validate` должен дать 0 ERROR. Предупреждения разбери (раздел 8).
5. **Отрендери:**
   ```bash
   $T -f timeline.jsonl render text -o timeline.txt
   $T -f timeline.jsonl render html -o timeline.html
   $T -f timeline.jsonl render sqlite -o timeline.db --session-id <task> --fresh   # <task> = имя папки задачи
   ```
   `--session-id` должен совпадать с именем задачи: тогда GUI склеит твою разметку с самим прогоном в одну сессию.
6. **Напиши `ANALYSIS.md`** (до 40 строк, со ссылками на T+мм:сс):
   - решена ли задача и почему;
   - где потеряно время и токены;
   - какие инциденты повлияли на исход;
   - что изменить в оркестрации.

### Ограничения режима A

- Ничего не меняй в `run/` и вне рабочей папки. `import-harness` и `transcript` читают прогон только на чтение.
- Не обращайся по сети к сервисам задачи и не запускай эксплойты и скрипты из переписок: ты только анализируешь.
- Работай автономно. Если чего-то не хватает (нет `pi-sessions`, обрезан ledger), отметь это в `ANALYSIS.md` и сделай, что возможно.

## 5. Режим B: журнал по ходу работы

Пиши запись сразу после факта, без `--t` (время = сейчас). Типичная последовательность:

```bash
export TIMELINE_FILE=timeline.jsonl
$T init "CTF Cup / pwn / heap" --task heap --goal "получить ctfcup{...}" --meta model=glm-5.3
$T agent P --role P --model zai/glm-5.3 --start
$T phase begin recon
$T dag add recon -o "разведка бинаря" --assignee E:recon          # --by P по умолчанию
$T dag add exploit -o "эксплойт" --deps recon --assignee E:exploit
$T dag status recon running
$T event E:recon "UAF в delete_note" -k milestone -s recon --to P --tokens 41000
$T hyp add "UAF в delete_note" -a P -s recon                      # → H1
$T hyp set H1 confirmed -e "show() после free печатает fd" -a E:recon
$T dag status recon done --by runtime
$T tokens E:recon 140000 --cumulative --cost 0.21
$T phase end recon
…
$T flag "ctfcup{...}" --status accepted -a E:exploit
$T outcome solved --flag "ctfcup{...}" --summary "UAF → tcache poisoning → FSOP"
$T agent P --stop --status done
$T end
```

Полный рабочий пример со всеми типами записей — `examples/demo_session.sh`.

- Токены пиши либо дельтами (`--tokens` у события, `tokens <агент> <дельта>`), либо нарастающим итогом (`tokens <агент> <итог> --cumulative`), не смешивая способы для одного агента.
- Лучше, чтобы журнал писал один процесс, например оркестратор. Записи независимы, и параллельная дозапись в один файл допустима. Но два процесса, выдающие id одновременно, могут выдать одинаковые `H<n>`/`I<n>`. Поэтому в параллельной записи задавай id явно и с префиксом своего шага: `hyp add … --hid H-recon-1`, `incident add … --iid I-recon-oom`.
- В конце выполни `validate` и рендеры из шага 5 режима A.

## 6. Правила разметки

1. **Сначала скелет, потом смысл.** Если есть импорт, начни с него; вручную дописывай только то, чего в источнике нет.
2. **Время — из источника.** В режиме A каждая запись идёт с `--t T+mm:ss` из `transcript`.
3. **Находки адресуй.** Milestone или result исполнителя, ушедший планировщику, пиши с `--to P`: в HTML появится стрелка отчёта. Решение P про шаг пиши с `-s <шаг>`, поручение — с `--to E:<шаг>`.
4. **Привязывай к шагу.** `-s <шаг>` должен быть узлом DAG: по нему работают фокус на шаге и статистика узлов.
5. **Гипотезы закрывай доказательством.** `hyp set … confirmed|refuted` без `-e` — это огрех, `validate` его отметит.
6. **`mitigate` ≠ `resolve`.** Обошли в сессии, а причина осталась — `mitigate`. Причина устранена — `resolve`. Открытыми считаются только `open` и `investigating`.
7. **Не засоряй.** Одно событие на значимый факт. Рутинные вызовы инструментов не нужны: токены и активность уже есть из импорта.
8. **Береги контекст.** Читай `transcript` с `-g`, `--since/--until`, `--last`. Свой журнал смотри через `render text --sections … --detail 0` или `render text --last 20 --agent E:x`.
9. **Перед рендером** запусти `validate`: ошибок быть не должно, предупреждения разобраны.

## 7. Инциденты: важность и категория

| категория | что относить |
|---|---|
| `harness` | дефект кода runtime: потеря stdout, ложный провал шага, неверная обработка протокола. **Только при доказательстве** |
| `orchestration` | решения P и устройство DAG: монолитный шаг, поздний параллелизм, неверный acceptance, бюджет не по задаче |
| `agent` | поведение исполнителя: зацикливание, игнор указаний, не отчитался, побочные эффекты |
| `model` | ответы модели: галлюцинации, сломанные tool calls, отказ |
| `infra` | провайдер и сеть: 5xx, перегрузка, лимиты |
| `env` | окружение задачи: нет инструмента, недоступен сервис |
| `tooling` | внешние инструменты агента (MCP, отладчик, декомпилятор) |
| `platform` | CTF-платформа |
| `other` | остальное |

Важность (`--sev`): `critical` — сорвало задачу; `high` — потеряно много времени или ложный провал шага; `medium` — заметная помеха; `low` — мелочь.

**Уроки прошлых разборов.** Из 28 инцидентов, которые аналитики пометили `harness`, настоящими дефектами оказались 8. Перед `-c harness` проверь:

- **Есть `run/STOP`, нет `run_finished`/`FINAL_REPORT`, в конце `EPIPE`.** Прогон остановил оператор, это не падение харнесса. Вердикт — `abandoned`.
- **`step … failed (timeout)`, `wall_timeout`.** Watchdog отработал заданный бюджет. Проблема в размере бюджета или монолитном шаге — `orchestration`.
- **Ход планировщика завершился «cancelled (exit 15)».** Это штатная отмена при `budget_exhausted` или `STOP`, не дефект.
- **«Отчёты E не доходят до P».** Сначала проверь, вызывал ли исполнитель `report_milestone` (`transcript -g report_milestone`). Не вызывал — это `agent`.
- **503, `overloaded`, rate limit.** Это `infra`, даже если харнесс корректно распознал ошибку.
- **`acceptance_failed` из-за не того пути или формата.** Это `orchestration` (P задал acceptance) или `agent` (E положил не туда).
- **Шаг помечен failed, хотя в `pi-sessions` есть финальный отчёт или `TASK_COMPLETE`.** Это действительно `harness` (потеря stdout), `high`.

**Автоинцидент оказался не дефектом.** Категорию поменять нельзя, поэтому закрой его с объяснением и при необходимости заведи правильный:

```bash
$T incident update I-recon-1 --status wontfix --note "не дефект харнесса: watchdog отработал wall_timeout 900 с"
$T incident add "монолитный шаг recon не уложился в бюджет" --sev high -c orchestration -s recon \
   --impact "потеряно 15 минут, шаг перезапущен" -d "см. I-recon-1" --t T+15:02
```

## 8. `validate`: что означают сообщения

ERROR (код возврата 1) надо исправить:

| сообщение | что делать |
|---|---|
| `record without time` | запись без `t`: добавь заново с `--t` |
| `no \`<поле>\` field — the record is ignored by renders` | запись, правленная руками, без обязательного поля: восстанови его или удали строку |
| `… is before session start` | неверное `--t`: проверь T+ (часы и минуты) |
| `phase end … without begin` | добавь `phase begin` с правильным временем |
| `node …: unknown dependency` | добавь узел-зависимость (`dag add`) или убери ребро (`dag dep-rm`) |
| ссылка на узел, гипотезу или инцидент раньше его создания | исправь время одной из записей |

WARN — вероятные огрехи, разбери каждый:

| сообщение | что делать |
|---|---|
| `milestone/result events of E/R have no --to` | добавь отчёт с `--to P`, если находка ушла планировщику |
| `hypothesis … without evidence` | `hyp set` с `-e` |
| `node … running without assignee` | `dag update <узел> --assignee E:<узел>` |
| `step … is not a DAG node` | исправь `-s` или добавь узел |
| `phase … never ended` | `phase end` (или оставь, если фаза шла до конца) |
| `incident … unknown category` | используй категорию из раздела 7 |
| `harness bug marked resolved without note` | дай `--note`, чем и где исправлено |
| `no outcome recorded yet` | поставь `outcome` |
| `… is after session end`, `dated in the future` | проверь `--t` |

## 9. Шпаргалка

```bash
# чтение
$T render text                                          # всё
$T render text --sections summary,agents,dag,dag-history,hyps,incidents,timeline --detail 0
$T render text --last 20 --agent E:exploit              # хвост ленты агента
$T render text --kinds milestone,decision,error --since +30m
$T transcript <pi-session.jsonl …> [-g regex] [--since T+..] [--until T+..] [--last N] [--results N] [--thinking] [--width N]

# агенты и события
$T agent <id> [--role P|E|R|C|operator|runtime|other] [--model m] [--parent P] [--label "…"] [--start | --stop [--status done|failed|killed]]
$T event <агент> "заголовок" [-k kind] [-s шаг] [--to агент] [-d "подробности"] [--tokens N] [--cost $] [--refs e1,H2] [--tags a,b]
$T note "заметка" [-a агент] [-s шаг] [-d "…"]

# DAG (--by — кто правит, по умолчанию P)
$T dag add <узел> -o "цель" [--deps a,b] [--assignee E:<узел>] [-d "инструкция"]
$T dag update <узел> [-o …] [--deps …] [--assignee …] [--note "…"]
$T dag status <узел> pending|running|done|failed|cancelled|blocked [--by runtime] [-r "причина"]
$T dag remove <узел> [-r "причина"]
$T dag dep-add <узел> <зависимости>        |  $T dag dep-rm <узел> <зависимости>

# токены
$T tokens <агент> <N> [--cumulative] [--in N --out N --cache-read N] [--cost $] [-s шаг]

# гипотезы
$T hyp add "утверждение" [-a агент] [-s шаг] [--hid H7]
$T hyp set <H> open|testing|confirmed|refuted|abandoned [-e "доказательство"] [-a агент]

# инциденты
$T incident add "заголовок" --sev low|medium|high|critical -c <категория> [-a агент] [-s шаг] [-d "…"] [--impact "…"] [--workaround "…"]
$T incident update <I> [--status open|investigating|mitigated|resolved|wontfix] [--sev …] [--note "…"] [--workaround "…"]
$T incident mitigate <I> --note "как обошли"     # причина осталась
$T incident resolve <I> --note "что исправлено"  # причина устранена

# флаг, вердикт, фазы, конец
$T flag "ctfcup{…}" [--status candidate|submitted|accepted|rejected] [-a агент] [--note "…"]
$T outcome solved|unsolved|partial|in_progress|abandoned [--flag "…"] [--summary "…"] [--by агент]
$T phase begin|end <имя> [-a агент] [--note "…"]
$T end

# импорт, проверка, рендеры
$T import-harness <run_dir> [--merge] [--bucket 60]
$T validate [--strict]
$T render text|html|json|jsonl [-o файл]
$T render sqlite -o timeline.db --session-id <task> [--fresh]
$T -f some.db sessions                                   # сессии в базе
$T -f some.db --session <id> render html                 # любой рендер из базы; база — только для чтения
```

У каждой команды записи есть `--t` (см. раздел 2).

## 10. Перед сдачей

- [ ] `validate` — 0 ERROR, каждое WARN разобрано;
- [ ] у каждого события, гипотезы и инцидента время из источника, а не `now` (режим A);
- [ ] каждый автоинцидент импорта проверен и при необходимости переквалифицирован (раздел 7);
- [ ] у находок исполнителей для P стоит `--to P`, у событий — `-s <шаг>`;
- [ ] есть фазы и вердикт;
- [ ] `timeline.txt`, `timeline.html`, `timeline.db` (с `--session-id <task>`) и `ANALYSIS.md` лежат в рабочей папке;
- [ ] в `run/` ничего не изменено.
