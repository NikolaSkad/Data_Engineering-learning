# Hadoop – beleške

**Sadržaj**

1. [Hadoop u jednoj rečenici](#1-hadoop-u-jednoj-rečenici)
2. [HDFS – osnovni pojmovi](#2-hdfs--osnovni-pojmovi)
3. [NameNode i metadata](#3-namenode-i-metadata)
4. [Čitanje i pisanje](#4-čitanje-i-pisanje)
5. [Blokovi i problem malih fajlova](#5-blokovi-i-problem-malih-fajlova)
6. [Otpornost na otkaze](#6-otpornost-na-otkaze)
7. [YARN i MapReduce](#7-yarn-i-mapreduce)
8. [Hive](#8-hive)
9. [Formati fajlova](#9-formati-fajlova)
10. [Brza pitanja za intervju](#10-brza-pitanja-za-intervju)

---

## 1. Hadoop u jednoj rečenici

> Hadoop je distribuirani sistem gde se podaci čuvaju podeljeni na više računara. NameNode zna gde je koji deo, pa se podaci mogu pronaći i paralelno obrađivati tamo gde se nalaze.

| Deo           | Uloga                                                      |
| ------------- | ---------------------------------------------------------- |
| **HDFS**      | **čuvanje** podataka na više računara                      |
| **YARN**      | **upravljanje resursima** (RAM, CPU) klastera              |
| **MapReduce** | **obrada** podataka paralelno, na računarima gde su podaci |

Radi na **običnom (commodity) hardveru**: jeftini računari, a pouzdanost dolazi iz replikacije, ne iz skupog hardvera.

---

## 2. HDFS – osnovni pojmovi

**Analogija – biblioteka:**

| HDFS         | Biblioteka                                                       |
| ------------ | ---------------------------------------------------------------- |
| **NameNode** | **katalog**: zna koja knjiga postoji i na kojoj je polici        |
| **DataNode** | **police**: tu su same knjige                                    |
| **Blok**     | jedan deo knjige (velika knjiga se iseče na delove od po 128 MB) |
| **Replika**  | kopija tog dela na drugoj polici, za slučaj da se jedna sruši    |
| **Metadata** | kartica u katalogu: naslov, autor, gde se nalazi                 |

**Pojmovi:**

- **Blok**: deo fajla, **najviše 128 MB**. Veliki fajl se deli na više blokova.
- **Replika**: kopija bloka. Podrazumevano ima **3 kopije** na 3 različita DataNode-a.
- **DataNode** (radnik): čuva **same podatke** (blokove) na svom disku. Ima ih mnogo.
- **NameNode** (šef): čuva **samo metadata**, tj. informacije **o** podacima. Ima jedan glavni.
- **Metadata**: „podaci o podacima“, npr. ime fajla, veličina, vlasnik i gde su mu blokovi.

Model **write-once, read-many**: fajl se zapiše jednom i posle se mnogo čita. Nema izmene usred fajla, samo dopisivanje na kraj.

---

## 3. NameNode i metadata

Na NameNode-u **nema nijednog bajta** samog fajla. On u **RAM-u** drži samo dve stvari:

### 3.1 Namespace – „stablo foldera“

Spisak svih foldera i fajlova, kao u Finder-u. Svaki fajl i folder je jedan **inode** (zapis) sa:

- putanjom: `/user/emp/data.csv`
- vlasnikom i pravima pristupa
- brojem replika: `3`
- **spiskom ID-jeva blokova**: `blk_1001, blk_1002`

### 3.2 Block map – „gde je koji blok“

Za svaki ID bloka, na kojim DataNode-ovima se nalazi:

```
blk_1001 → datanode1, datanode2, datanode3
blk_1002 → datanode2, datanode3, datanode4
```

Ovo **nisu pokazivači na memoriju**, nego **ID blokova** i **lokacije blokova** (block locations).

### 3.3 Kako se metadata čuva da se ne izgubi

| Šta                  | Gde                       | Opis                                                          |
| -------------------- | ------------------------- | ------------------------------------------------------------- |
| **fsimage**          | disk NameNode-a           | snimak celog namespace-a u jednom trenutku                    |
| **edit log**         | disk NameNode-a           | dnevnik svih izmena posle poslednjeg snimka                   |
| **lokacije blokova** | **ne čuvaju se na disku** | posle restarta ih DataNode-ovi ponovo jave (**block report**) |

Pri startu NameNode učita `fsimage`, primeni `edit log` i čeka block report-e. Dok ne sazna dovoljno lokacija, nalazi se u **safe mode-u**, gde je samo za čitanje.

---

## 4. Čitanje i pisanje

**Čitanje:**

1. Klijent pita **NameNode**: „gde su blokovi fajla X?“
2. NameNode vrati spisak: „blok 1 je na DN1, DN2 i DN3...“
3. Klijent čita blokove **direktno sa DataNode-ova** (sa najbližeg).

**Pisanje:**

1. Klijent pita **NameNode** gde da upiše blok.
2. NameNode vrati 3 DataNode-a.
3. Klijent šalje blok **prvom**, on ga prosleđuje **drugom**, pa drugi **trećem** (**pipeline**).

**Podaci nikad ne prolaze kroz NameNode**, samo pitanja „gde je?“.

---

## 5. Blokovi i problem malih fajlova

### 128 MB je maksimum, a ne fiksna veličina

Blok je **običan fajl na disku DataNode-a**, velik tačno koliko podataka ima. Ništa se ne rezerviše unapred.

| Fajl   | Blokovi na disku                |
| ------ | ------------------------------- |
| 1 MB   | 1 blok od **1 MB** (ne 128 MB!) |
| 128 MB | 1 blok od 128 MB                |
| 300 MB | 128 + 128 + **44 MB**           |

**Zašto je blok tako veliki?** Manje blokova znači manje metadata na NameNode-u, a čitanje velikih komada je efikasnije.

### Problem malih fajlova

Disk nije problem. Problem je **RAM NameNode-a**: **svaki fajl i svaki blok** zauzima oko **150 B** metadata, bez obzira na veličinu.

| Podaci                            | Unosi u RAM-u NameNode-a  |
| --------------------------------- | ------------------------- |
| 1 fajl od 1 GB (8 blokova)        | 1 + 8 = **9**             |
| 1.000 fajlova od 1 MB (isti 1 GB) | 1.000 + 1.000 = **2.000** |

Ista količina podataka, a oko 200 puta više memorije. Uz to, svaki mali fajl je poseban task u obradi, pa je i obrada sporija.

**Rešenje:** spojiti male fajlove u veće (ili koristiti SequenceFile, Parquet ili ORC).

---

## 6. Otpornost na otkaze

| Mehanizam          | Šta radi                                                                            |
| ------------------ | ----------------------------------------------------------------------------------- |
| **Replikacija**    | svaki blok na 3 DataNode-a                                                          |
| **Heartbeat**      | DataNode na ~3 s javlja „živ sam“; ako ćuti, smatra se mrtvim                       |
| **Re-replikacija** | kad DataNode padne, NameNode pravi nove kopije njegovih blokova na drugim čvorovima |
| **Rack awareness** | replike na različitim rack-ovima (ormarima), pa ni pad celog rack-a ne gubi podatke |
| **NameNode HA**    | Active + Standby NameNode; ako aktivni padne, standby preuzima                      |

**Zamka:** **Secondary NameNode NIJE backup.** On samo povremeno spaja `fsimage` i `edit log` (checkpoint), da edit log ne bi rastao beskonačno.

NameNode je **single point of failure (SPOF)**. Bez njega niko ne zna gde su blokovi, pa se zato koristi HA.

---

## 7. YARN i MapReduce

### YARN – ko deli resurse

| Komponenta                        | Uloga                                           |
| --------------------------------- | ----------------------------------------------- |
| **ResourceManager** (šef)         | deli RAM i CPU celog klastera                   |
| **NodeManager** (na svakom čvoru) | pokreće poslove na svom čvoru i javlja stanje   |
| **ApplicationMaster** (po poslu)  | vodi jedan posao: traži resurse i prati taskove |

Od Hadoop 2.0 YARN je zamenio JobTracker/TaskTracker. Na njemu rade i Spark i Tez, ne samo MapReduce.

### MapReduce – kako se obrađuje

```
Input split → Map → Shuffle & Sort → Reduce → rezultat na HDFS
```

- **Input split**: deo ulaza (obično = 1 blok). **Jedan map task po split-u.**
- **Map**: svaki čvor obradi svoj deo i emituje parove `(ključ, vrednost)`.
- **Shuffle & sort**: parovi sa istim ključem idu kod istog reducer-a. Ide preko mreže, pa je **najskuplji deo**.
- **Reduce**: spaja vrednosti po ključu u konačan rezultat.

„Map“ **nije mapa podataka**, nego korak obrade.

**Data locality**: kod se šalje tamo gde su podaci, a ne podaci kod koda. Pomeranje programa (KB) je jeftinije od pomeranja podataka (GB).

**Blok vs split**: blok je **fizička** podela (HDFS), a split je **logička** podela (MapReduce).

---

## 8. Hive

**SQL nad fajlovima na HDFS-u.** HQL upit se prevodi u MapReduce, Tez ili Spark posao. Namenjen je **analitici (OLAP)**, ne transakcijama.

| Pojam               | Objašnjenje                                                                                                                |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| **Metastore**       | baza (kod nas Postgres) sa šemama tabela i lokacijama fajlova; koristi ga i Spark                                          |
| **Schema-on-read**  | šema se primenjuje pri **čitanju**; `LOAD DATA` ne proverava podatke (zato smo dobili `NULL` kad razdvajač nije odgovarao) |
| **Managed tabela**  | Hive je vlasnik (`/user/hive/warehouse`), pa `DROP` briše i fajlove                                                        |
| **External tabela** | `DROP` briše samo šemu, a fajlovi ostaju                                                                                   |
| **Particija**       | svaka vrednost je poseban folder (`ds=2008-08-15/`); `WHERE` po particiji čita samo taj folder (**partition pruning**)     |
| **SerDe**           | kako se fajl čita i piše (npr. `OpenCSVSerde` za CSV sa navodnicima)                                                       |

`LOAD DATA INPATH` **premešta** fajl sa HDFS-a, a `LOAD DATA LOCAL INPATH` **kopira** sa lokalnog diska.

---

## 9. Formati fajlova

| Format                | Tip             | Kad                                                       |
| --------------------- | --------------- | --------------------------------------------------------- |
| **Text / CSV / JSON** | redovi, tekst   | jednostavno i čitljivo, ali sporo i veliko                |
| **Avro**              | redovi, binarno | kad se **šema često menja** (schema evolution), streaming |
| **Parquet**           | **kolonski**    | analitika, **standard u Spark-u**                         |
| **ORC**               | **kolonski**    | analitika, najbolji za Hive                               |

**Zašto su kolonski formati brži za analitiku:**

- čitaju se samo kolone koje upit traži
- bolja kompresija, jer su slične vrednosti zajedno
- metapodaci (min/max) omogućavaju preskakanje delova fajla

---

## 10. Brza pitanja za intervju

| Pitanje                             | Odgovor                                                                                                               |
| ----------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| Šta čuva NameNode?                  | Samo **metadata**: namespace (fajlovi i folderi) i lokacije blokova. Nikad same podatke.                              |
| Šta čuva DataNode?                  | **Same blokove** podataka.                                                                                            |
| Fajl od 1 MB zauzima 128 MB?        | **Ne**, zauzima 1 MB. Ali troši metadata na NameNode-u kao i veliki fajl.                                             |
| 1 GB sa replikacijom 3?             | **3 GB** na disku.                                                                                                    |
| Zašto mali fajlovi smetaju?         | Pune **RAM NameNode-a** i prave previše taskova.                                                                      |
| Da li podaci idu kroz NameNode?     | **Ne.** Klijent čita i piše direktno sa DataNode-ova.                                                                 |
| Secondary NameNode = backup?        | **Ne.** Samo radi checkpoint (`fsimage` + `edit log`).                                                                |
| Šta se desi kad DataNode padne?     | NameNode primeti da nema heartbeat-a i pravi nove replike na drugim čvorovima.                                        |
| Šta je safe mode?                   | Posle starta NameNode je samo za čitanje, dok mu DataNode-ovi ne jave blokove.                                        |
| HDFS vs baza podataka?              | HDFS čuva fajlove i nema indekse ni transakcije. Brz je za skeniranje ogromnih podataka, a spor za pojedinačne upite. |
| Šta se danas koristi umesto HDFS-a? | Object storage (S3, ADLS, GCS) + Spark + Parquet/Delta/Iceberg. Koncepti ostaju isti.                                 |

**Ključni termini:** blok, replika, NameNode, DataNode, metadata, namespace, inode, block ID, block locations, fsimage, edit log, heartbeat, block report, safe mode, rack awareness, data locality, input split, shuffle, schema-on-read, partition pruning.
