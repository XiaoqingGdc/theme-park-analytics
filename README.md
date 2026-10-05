# 🎢 Theme Park Analytics

Analyse des temps d'attente de 8 parcs d'attractions en Europe et aux États-Unis, à partir de données collectées automatiquement toutes les 10 minutes.

> 🚧 Projet en cours : collecte active depuis octobre 2026, tableau de bord Power BI à venir.

## Indicateurs clés (à venir)

- **Attente moyenne** par parc et par attraction
- **Heure de pointe** (en heure locale de chaque parc)
- **Attraction la plus fréquentée**
- **Impact de la météo** sur les temps d'attente

## Architecture

```
ThemeParks.wiki API     Open-Meteo API
         └───────┬───────────┘
                 ▼
     Apache Airflow (Docker)
     DAG toutes les 10 min, 8h–24h heure locale
                 │
        ┌────────┴────────┐
        ▼                 ▼
  JSON brut (.gz)   CSV structuré
                          │
                          ▼
                    SQL (à venir)
                          │
                          ▼
                Power BI (à venir)
```

## Parcs suivis

| Destination | Parcs |
|---|---|
| 🇫🇷 Disneyland Paris | Disneyland Park, Disney Adventure World |
| 🇫🇷 Parc Astérix | Parc Astérix |
| 🇩🇪 Europa-Park | Europa-Park |
| 🇩🇪 Phantasialand | Phantasialand |
| 🇬🇧 Alton Towers | Alton Towers |
| 🇬🇧 LEGOLAND Windsor | LEGOLAND Windsor |
| 🇺🇸 Walt Disney World | Magic Kingdom, EPCOT, Hollywood Studios, Animal Kingdom |
| 🇺🇸 Universal Orlando | Universal Studios Florida, Islands of Adventure, Epic Universe |

## Stack

Python · Apache Airflow 3 · Docker · Power BI

## Lancer le projet

```bash
docker compose up -d
```

Interface Airflow : http://localhost:8080

## Structure

```
theme-park-analytics/
├── airflow/dags/        # Pipeline de collecte
├── data/                # Données collectées (non versionnées)
├── docker-compose.yml
└── README.md
```

## Auteur

**Xiaoqing ZHOU GRANDCOING** · Data Analyst
[LinkedIn](https://www.linkedin.com/in/xiaoqingzhougrandcoing) · [Portfolio](https://xiaoqinggdc.github.io/)
