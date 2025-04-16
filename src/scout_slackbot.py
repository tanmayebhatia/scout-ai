from slack_bolt.async_app import AsyncApp
from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler
import os
from dotenv import load_dotenv
import logging
from openai import AsyncOpenAI
from pinecone import Pinecone, ServerlessSpec
from typing import List, Dict
import aiohttp
import asyncio

class ScoutSlackBot:
    def __init__(self):
        load_dotenv()
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger(__name__)
        
        # Debug environment
        self.logger.info(f"OpenAI Key exists: {bool(os.environ.get('OPENAI_API_KEY'))}")
        self.logger.info(f"OpenAI Key starts with: {os.environ.get('OPENAI_API_KEY')[:10]}...")
        
        # Initialize non-async clients
        self.slack_app = AsyncApp(token=os.environ.get("SLACK_BOT_TOKEN"))
        self.openai_client = AsyncOpenAI()
        
        # Initialize Pinecone
        self.pc = Pinecone(api_key=os.environ.get("PINECONE_API_KEY"))
        self.index = self.pc.Index(os.environ.get("PINECONE_INDEX"))
        self.logger.info("✅ Pinecone initialized")
        
        # Will be initialized in setup()
        self.socket_handler = None
        self.session = None
    
    async def setup(self):
        """Initialize async components"""
        if self.socket_handler is None:
            self.socket_handler = AsyncSocketModeHandler(
                app=self.slack_app,
                app_token=os.environ.get("SLACK_APP_TOKEN")
            )
            self._register_handlers()
    
    def _register_handlers(self):
        @self.slack_app.event("app_mention")
        async def handle_mention(body, say):
            self.logger.info("🎯 Entering handle_mention")
            self.logger.info(f"Full event body: {body}")
            
            try:
                # Get thread_ts for threading
                thread_ts = body['event'].get('thread_ts') or body['event']['ts']
                
                # Extract query and remove bot mention
                text = body['event']['text']
                self.logger.info(f"Raw text: {text}")
                
                query = text.split(">", 1)[1].strip()
                self.logger.info(f"Extracted query: {query}")
                
                # Show we're working (in thread)
                self.logger.info("Sending initial response")
                await say(
                    text=f"🔍 Starting search for: *{query}*",
                    thread_ts=thread_ts
                )
                
                # Get embedding
                self.logger.info("Getting embedding")
                embedding = await self.get_embedding(query)
                self.logger.info("Got embedding")
                
                # Search Pinecone
                self.logger.info("Searching Pinecone")
                results = await self.search_similar(embedding)
                self.logger.info(f"Got {len(results)} results")
                
                if not results:
                    await say(
                        text="No matches found 😕",
                        thread_ts=thread_ts
                    )
                    return
                
                # Format response with minimal metadata
                response = "*🎯 Here are the top matches:*\n\n"
                for i, match in enumerate(results, 1):
                    metadata = match.metadata
                    score = round(match.score * 100, 2)
                    
                    # Extract only needed fields
                    name = metadata.get('full_name', 'Unknown')
                    headline = metadata.get('headline', 'No headline')
                    linkedin = metadata.get('linkedin_url', '#')
                    
                    # Get just the role from headline
                    current_role = headline.split(" | ")[0] if " | " in headline else headline
                    
                    # Format minimal response - name as LinkedIn link and current role
                    response += f"{i}. <https://linkedin.com/in/{linkedin}|{name}>\n"
                    response += f"   {current_role}\n\n"
                
                # Send final response in same thread
                self.logger.info("Sending final response")
                await say(
                    text=response,
                    thread_ts=thread_ts
                )
                
            except Exception as e:
                self.logger.error(f"Search error: {e}", exc_info=True)
                await say(
                    text=f"Sorry, I encountered an error: {str(e)} 😕",
                    thread_ts=thread_ts
                )
    
    async def get_embedding(self, text: str) -> List[float]:
        """Get OpenAI embedding for text"""
        response = await self.openai_client.embeddings.create(
            model="text-embedding-3-small",
            input=text
        )
        return response.data[0].embedding
    
    async def search_similar(self, query_embedding: List[float], top_k: int = 5) -> List[Dict]:
        """Search Pinecone for similar vectors"""
        results = self.index.query(
            vector=query_embedding,
            top_k=top_k,
            include_metadata=True
        )
        # Debug metadata
        self.logger.info("Pinecone results metadata:")
        for match in results['matches']:
            self.logger.info(f"Match metadata: {match.metadata}")
        return results['matches']
    
    async def start(self):
        """Start socket mode"""
        self.logger.info("Starting socket mode...")
        await self.setup()
        await self.socket_handler.start_async()
    
    async def cleanup(self):
        """Cleanup resources"""
        self.logger.info("Starting cleanup...")
        try:
            # Close socket handler first
            if hasattr(self, 'socket_handler'):
                await self.socket_handler.close()
            
            # Then close session
            if self.session and not self.session.closed:
                await self.session.close()
            
            self.logger.info("Cleanup complete")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}") 