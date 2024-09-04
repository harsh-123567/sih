import scrapy
from pymongo import MongoClient
from transformers import pipeline
from bs4 import BeautifulSoup
import re

class KeywordSpider(scrapy.Spider):
    name = 'keyword_spider'

    def __init__(self, *args, **kwargs):
        super(KeywordSpider, self).__init__(*args, **kwargs)
        self.keywords = [
            'earthquake', 'tsunami', 'flood', 'hurricane', 'cyclone', 'tornado', 
            'landslide', 'drought', 'volcanic eruption', 'avalanche', 'fire', 
            'explosion', 'chemical spill', 'nuclear accident', 'building collapse', 
            'oil spill', 'wildfire', 'severe storm', 'extreme heat', 'heavy rainfall', 
            'ice storm', 'epidemic', 'pandemic'
        ]
        self.start_urls = [
            'https://timesofindia.indiatimes.com/',
            'https://www.thehindu.com/',
            'https://indianexpress.com/',
            'https://www.hindustantimes.com/',
            'https://www.ndtv.com/',
            'https://www.bbc.com/news/world/asia/india',
            'https://economictimes.indiatimes.com/',
            'https://www.deccanherald.com/',
            'https://www.newindianexpress.com/',
            'https://www.dailythanthi.com/',
            'https://www.telegraphindia.com/',
            'https://www.sakaltimes.com/',
            'https://zeenews.india.com/',
            'https://www.news18.com/',
            'https://www.thequint.com/',
            'https://scroll.in/',
            'https://www.aljazeera.com/where/india/'
        ]
        self.client = MongoClient('mongodb+srv://harshgoyal2408:pro_hacker@sih.gxr5q.mongodb.net/')
        self.db = self.client['webcrawler']
        self.collection = self.db['disaster_news']
        self.text_classifier = pipeline('text-classification', model='distilbert-base-uncased-finetuned-sst-2-english')

    def parse(self, response):
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract main content using common HTML tags
        paragraphs = soup.find_all('p')
        article_content = ' '.join(p.get_text() for p in paragraphs)
        
        # Clean and preprocess article content
        cleaned_content = self.clean_text(article_content)
        
        # Check if the cleaned content contains any of the keywords
        if self.is_relevant(cleaned_content):
            # Classify the content
            classified_content = self.text_classifier(cleaned_content)
            if any(classification['label'] == 'POSITIVE' for classification in classified_content):
                item = {
                    'url': response.url,
                    'content': cleaned_content
                }
                self.collection.insert_one(item)

        # Follow links to other pages if they are likely disaster-related
        for next_page in response.css('a::attr(href)').getall():
            if self.is_disaster_related(next_page):
                yield response.follow(next_page, self.parse)

    def clean_text(self, text):
        """Clean and preprocess the text."""
        text = text.lower()  # Convert to lowercase
        text = re.sub(r'\s+', ' ', text)  # Remove excessive whitespace
        text = re.sub(r'[^\w\s]', '', text)  # Remove punctuation
        return text

    def is_relevant(self, text):
        """Check if the text contains any relevant keywords."""
        return any(keyword in text for keyword in self.keywords)

    def is_disaster_related(self, url):
        """Check if the URL is likely to lead to disaster-related news."""
        return any(keyword in url.lower() for keyword in self.keywords)
