# vtsm-para
Match a video's audio to the tracks of it's soundtrack

**GOAL:** This tool was designed to match the audio track of an anime's episodes to the OSTs of the anime.

**How it works:** 
1. Extract the audio track from the episodes using _moviepy_.
2. Separate the voices from the audio using _spleeter_.
3. Divide each episode's audio into 10s chunks.
4. Create a fingerprint database from every chunk using _dejavu_'s fingerprinting method.
5. Create a fingerpirnt database from the OSTs.
6. Create a matching database (match every 10s chunk to a soundtrack from the OST
