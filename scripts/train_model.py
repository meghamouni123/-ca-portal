"""
Train the CA classifier model.
Run from ca_portal/ directory: python scripts/train_model.py
"""
import sys
import logging

sys.path.insert(0, 'backend')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

from classifier import train_classifier

if __name__ == '__main__':
    print("Training CA classifier...")
    acc = train_classifier()
    print(f"\nDone. Accuracy: {acc:.4f}")
    print("Models saved to models/")
