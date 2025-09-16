start: 
	sudo astro dev start --image-name mytek-data-pipeline_f7f14c/airflow

stop: 
	sudo astro dev stop

restart: 
	sudo astro dev restart --image-name mytek-data-pipeline_f7f14c/airflow

dashboard: 
	sudo docker exec -it 3e98f0af7677 streamlit run include/streamlitApp/plot_gold.py