from fastapi import FastAPI, Request
from slack_bolt import App
from slack_bolt.adapter.fastapi import SlackRequestHandler
import os
from dotenv import load_dotenv

class ScoutBot:
    def __init__(self):
        load_dotenv()
        
        # Initialize Slack app
        self.slack_app = App(
            token=os.environ.get("SLACK_BOT_TOKEN"),
            signing_secret=os.environ.get("SLACK_SIGNING_SECRET")
        )
        self.handler = SlackRequestHandler(self.slack_app)
        
        # Register event handlers
        self._register_handlers()
    
    def _register_handlers(self):
        @self.slack_app.event("app_mention")
        def handle_mention(event, say):
            say(f"Hello! Scout here. You mentioned me in <#{event['channel']}>") 