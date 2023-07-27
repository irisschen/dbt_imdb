
DATA_DIR=data
FILES=$(DATA_DIR)/name.basics.tsv.gz $\
			$(DATA_DIR)/title.basics.tsv.gz $\
			$(DATA_DIR)/title.akas.tsv.gz $\
			$(DATA_DIR)/title.crew.tsv.gz $\
			$(DATA_DIR)/title.episode.tsv.gz $\
			$(DATA_DIR)/title.princicles.tsv.gz

all: $(FILES)

$(DATA_DIR)/%.tsv.gz:
	wget -P $(DATA_DIR) https://datasets.imdbws.com/$(@F)

clean:
	rm -f $(FILES)
